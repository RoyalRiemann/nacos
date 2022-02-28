/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.core.distributed.raft;

import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.InternetAddressUtil;
import com.alibaba.nacos.common.utils.LoggerUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;
import com.alibaba.nacos.consistency.RequestProcessor;
import com.alibaba.nacos.consistency.SerializeFactory;
import com.alibaba.nacos.consistency.Serializer;
import com.alibaba.nacos.consistency.cp.RequestProcessor4CP;
import com.alibaba.nacos.consistency.entity.ReadRequest;
import com.alibaba.nacos.consistency.entity.Response;
import com.alibaba.nacos.consistency.exception.ConsistencyException;
import com.alibaba.nacos.core.distributed.raft.exception.DuplicateRaftGroupException;
import com.alibaba.nacos.core.distributed.raft.exception.JRaftException;
import com.alibaba.nacos.core.distributed.raft.exception.NoLeaderException;
import com.alibaba.nacos.core.distributed.raft.exception.NoSuchRaftGroupException;
import com.alibaba.nacos.core.distributed.raft.utils.FailoverClosure;
import com.alibaba.nacos.core.distributed.raft.utils.FailoverClosureImpl;
import com.alibaba.nacos.core.distributed.raft.utils.JRaftConstants;
import com.alibaba.nacos.core.distributed.raft.utils.JRaftUtils;
import com.alibaba.nacos.core.distributed.raft.utils.RaftExecutor;
import com.alibaba.nacos.core.distributed.raft.utils.RaftOptionsBuilder;
import com.alibaba.nacos.core.monitor.MetricsMonitor;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.CliServiceImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Message;
import org.springframework.util.CollectionUtils;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * JRaft server instance, away from Spring IOC management.
 *
 * <p>
 * Why do we need to create a raft group based on the value of LogProcessor group (), that is, each function module has
 * its own state machine. Because each LogProcessor corresponds to a different functional module, such as Nacos's naming
 * module and config module, these two modules are independent of each other and do not affect each other. If we have
 * only one state machine, it is equal to the log of all functional modules The processing is loaded together. Any
 * module that has an exception during the log processing and a long block operation will affect the normal operation of
 * other functional modules.
 * </p>
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
@SuppressWarnings("all")
public class JRaftServer {
    
    // Existential life cycle
    
    static {
        // Set bolt buffer 设置阈值吧
        // System.getProperties().setProperty("bolt.channel_write_buf_low_water_mark", String.valueOf(64 * 1024 * 1024));
        // System.getProperties().setProperty("bolt.channel_write_buf_high_water_mark", String.valueOf(256 * 1024 * 1024));

        //128m~256m
        System.getProperties().setProperty("bolt.netty.buffer.low.watermark", String.valueOf(128 * 1024 * 1024));
        System.getProperties().setProperty("bolt.netty.buffer.high.watermark", String.valueOf(256 * 1024 * 1024));
    }

    //Rpc服务
    private RpcServer rpcServer;

    //直接调用leader进行连接操作的服务类
    private CliClientServiceImpl cliClientService;

    //对一些业务进行了封装，调用CliClientServiceImpl操作的类
    private CliService cliService;
    
    // 组和组服务，以及复制状态机，以及节点
    private Map<String, RaftGroupTuple> multiRaftGroup = new ConcurrentHashMap<>();

    // 是否已经启动
    private volatile boolean isStarted = false;

    // 是否已经关闭
    private volatile boolean isShutdown = false;

    // 配置文件
    private Configuration conf;

    // rpc处理器
    private RpcProcessor userProcessor;

    // Node命令
    private NodeOptions nodeOptions;
    
    private Serializer serializer;

    // 处理器
    private Collection<RequestProcessor4CP> processors = Collections.synchronizedSet(new HashSet<>());

    // 自己IP
    private String selfIp;

    // 自己port
    private int selfPort;

    //RaftConfig Nacos定义的配置文件
    private RaftConfig raftConfig;

    //代表一个参与者
    private PeerId localPeerId;

    //失败重试次数
    private int failoverRetries;

    // RPC超时时间
    private int rpcRequestTimeoutMs;

    //初始化
    public JRaftServer() {
        this.conf = new Configuration();
    }

    //设置失败重试次数
    public void setFailoverRetries(int failoverRetries) {
        this.failoverRetries = failoverRetries;
    }

    //初始化
    void init(RaftConfig config) {
        this.raftConfig = config;
        this.serializer = SerializeFactory.getDefault();//默认hession
        Loggers.RAFT.info("Initializes the Raft protocol, raft-config info : {}", config);
        RaftExecutor.init(config);//初始化raft的各种执行器
        
        final String self = config.getSelfMember();
        String[] info = InternetAddressUtil.splitIPPortStr(self);
        selfIp = info[0];
        selfPort = Integer.parseInt(info[1]);
        //复制组中的参与者?
        localPeerId = PeerId.parsePeer(self);
        nodeOptions = new NodeOptions();
        
        // Set the election timeout time. The default is 5 seconds. leader在超时事件内就没有发送心跳到local，本地发起投票，状态变更为candidate
        int electionTimeout = Math.max(ConvertUtils.toInt(config.getVal(RaftSysConstants.RAFT_ELECTION_TIMEOUT_MS),
                RaftSysConstants.DEFAULT_ELECTION_TIMEOUT), RaftSysConstants.DEFAULT_ELECTION_TIMEOUT);
        
        rpcRequestTimeoutMs = ConvertUtils.toInt(raftConfig.getVal(RaftSysConstants.RAFT_RPC_REQUEST_TIMEOUT_MS),
                RaftSysConstants.DEFAULT_RAFT_RPC_REQUEST_TIMEOUT_MS);

        //是否全局的选举定时、是否全局投票定时、是否全局降速定时、是否全局分享的快照定时器
        nodeOptions.setSharedElectionTimer(true);
        nodeOptions.setSharedVoteTimer(true);
        nodeOptions.setSharedStepDownTimer(true);
        nodeOptions.setSharedSnapshotTimer(true);
        
        nodeOptions.setElectionTimeoutMs(electionTimeout);//选举超时
        RaftOptions raftOptions = RaftOptionsBuilder.initRaftOptions(raftConfig);
        //从代码看，这个步骤估计是设计者后面补充的
        nodeOptions.setRaftOptions(raftOptions);
        // open jraft node metrics record function,建模是打开的
        nodeOptions.setEnableMetrics(true);
        
        CliOptions cliOptions = new CliOptions();

        //这两个服务暂时不知道是什么用处
        this.cliService = RaftServiceFactory.createAndInitCliService(cliOptions); //启动cli服务
        this.cliClientService = (CliClientServiceImpl) ((CliServiceImpl) this.cliService).getCliClientService();
    }
    
    synchronized void start() {
        if (!isStarted) {
            Loggers.RAFT.info("========= The raft protocol is starting... =========");
            try {
                // init raft group node
                com.alipay.sofa.jraft.NodeManager raftNodeManager = com.alipay.sofa.jraft.NodeManager.getInstance();
                for (String address : raftConfig.getMembers()) {
                    PeerId peerId = PeerId.parsePeer(address);
                    conf.addPeer(peerId); //主要就是把peerId加到conf中了
                    raftNodeManager.addAddress(peerId.getEndpoint());
                }
                //初始化的配置文件
                nodeOptions.setInitialConf(conf);

                //初始化RPCservice,包括各种初始化参数，同时也包括各种处理器processor
                rpcServer = JRaftUtils.initRpcServer(this, localPeerId);

                //GrpcServer init
                if (!this.rpcServer.init(null)) {
                    Loggers.RAFT.error("Fail to init [BaseRpcServer].");
                    throw new RuntimeException("Fail to init [BaseRpcServer].");
                }
                
                // Initialize multi raft group service framework
                isStarted = true;
                //
                createMultiRaftGroup(processors);
                Loggers.RAFT.info("========= The raft protocol start finished... =========");
            } catch (Exception e) {
                Loggers.RAFT.error("raft protocol start failure, cause: ", e);
                throw new JRaftException(e);
            }
        }
    }
    
    synchronized void createMultiRaftGroup(Collection<RequestProcessor4CP> processors) {
        // There is no reason why the LogProcessor cannot be processed because of the synchronization
        // 同步问题
        if (!this.isStarted) {
            this.processors.addAll(processors);
            return;
        }
        
        final String parentPath = Paths.get(EnvUtil.getNacosHome(), "data/protocol/raft").toString();
        
        for (RequestProcessor4CP processor : processors) {
            //组？
            final String groupName = processor.group();
            if (multiRaftGroup.containsKey(groupName)) {
                throw new DuplicateRaftGroupException(groupName);
            }
            
            // Ensure that each Raft Group has its own configuration and NodeOptions
            Configuration configuration = conf.copy();
            NodeOptions copy = nodeOptions.copy();
            //每个组单独建文件路径
            JRaftUtils.initDirectory(parentPath, groupName, copy);
            
            // Here, the LogProcessor is passed into StateMachine, and when the StateMachine
            // triggers onApply, the onApply of the LogProcessor is actually called,状态机也是单独的
            NacosStateMachine machine = new NacosStateMachine(this, processor);
            
            copy.setFsm(machine);
            copy.setInitialConf(configuration);
            
            // Set snapshot interval, default 1800 seconds
            int doSnapshotInterval = ConvertUtils.toInt(raftConfig.getVal(RaftSysConstants.RAFT_SNAPSHOT_INTERVAL_SECS),
                    RaftSysConstants.DEFAULT_RAFT_SNAPSHOT_INTERVAL_SECS);
            
            // If the business module does not implement a snapshot processor, cancel the snapshot
            // 如果没有实现快照处理器，则取消快照:快照处理器和日志处理器同级别
            doSnapshotInterval = CollectionUtils.isEmpty(processor.loadSnapshotOperate()) ? 0 : doSnapshotInterval;
            
            copy.setSnapshotIntervalSecs(doSnapshotInterval);
            Loggers.RAFT.info("create raft group : {}", groupName);
            //nacos的Raft按组服务的
            RaftGroupService raftGroupService = new RaftGroupService(groupName, localPeerId, copy, rpcServer, true);
    
            // Because BaseRpcServer has been started before, it is not allowed to start again here
            //获取node，并且初始化节点
            Node node = raftGroupService.start(false);
            machine.setNode(node);//将node放到复制状态机
            RouteTable.getInstance().updateConfiguration(groupName, configuration);

            //注册自己到集群,这里的骚操作时，首先从peers中找到一条能够通的，让然后找到leader，然后再把数据打包从leader处进行add
            RaftExecutor.executeByCommon(() -> registerSelfToCluster(groupName, localPeerId, configuration));
            
            // Turn on the leader auto refresh for this group--开启leader自动刷新?
            Random random = new Random();
            //选举超时+随机时间,刷新leader和configuration
            long period = nodeOptions.getElectionTimeoutMs() + random.nextInt(5 * 1000);
            RaftExecutor.scheduleRaftMemberRefreshJob(() -> refreshRouteTable(groupName),
                    nodeOptions.getElectionTimeoutMs(), period, TimeUnit.MILLISECONDS);
            multiRaftGroup.put(groupName, new RaftGroupTuple(node, processor, raftGroupService, machine));
        }
    }

    //这哥们的代码真绕
    CompletableFuture<Response> get(final ReadRequest request) {
        final String group = request.getGroup();
        CompletableFuture<Response> future = new CompletableFuture<>();
        final RaftGroupTuple tuple = findTupleByGroup(group);
        if (Objects.isNull(tuple)) {
            future.completeExceptionally(new NoSuchRaftGroupException(group));
            return future;
        }
        final Node node = tuple.node;
        final RequestProcessor processor = tuple.processor;
        try {
            //ReadIndexClosure 每个2s执行一次
            node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
                @Override
                public void run(Status status, long index, byte[] reqCtx) {
                    if (status.isOk()) {
                        try {
                            Response response = processor.onRequest(request);
                            future.complete(response);
                        } catch (Throwable t) {
                            MetricsMonitor.raftReadIndexFailed();
                            future.completeExceptionally(new ConsistencyException(
                                    "The conformance protocol is temporarily unavailable for reading", t));
                        }
                        return;
                    }
                    MetricsMonitor.raftReadIndexFailed();
                    Loggers.RAFT.error("ReadIndex has error : {}, go to Leader read.", status.getErrorMsg());
                    MetricsMonitor.raftReadFromLeader();
                    readFromLeader(request, future);
                }
            });
            return future;
        } catch (Throwable e) {
            MetricsMonitor.raftReadFromLeader();
            Loggers.RAFT.warn("Raft linear read failed, go to Leader read logic : {}", e.toString());
            // run raft read
            readFromLeader(request, future);
            return future;
        }
    }
    
    public void readFromLeader(final ReadRequest request, final CompletableFuture<Response> future) {
        commit(request.getGroup(), request, future);
    }

    //put数据
    public CompletableFuture<Response> commit(final String group, final Message data,
            final CompletableFuture<Response> future) {
        LoggerUtils.printIfDebugEnabled(Loggers.RAFT, "data requested this time : {}", data);
        final RaftGroupTuple tuple = findTupleByGroup(group);
        if (tuple == null) {
            future.completeExceptionally(new IllegalArgumentException("No corresponding Raft Group found : " + group));
            return future;
        }
        
        FailoverClosureImpl closure = new FailoverClosureImpl(future);
        
        final Node node = tuple.node;
        if (node.isLeader()) {
            // The leader node directly applies this request
            applyOperation(node, data, closure);
        } else {
            // Forward to Leader for request processing
            invokeToLeader(group, data, rpcRequestTimeoutMs, closure);
        }
        return future;
    }
    
    /**
     * Add yourself to the Raft cluster
     *
     * @param groupId raft group
     * @param selfIp  local raft node address
     * @param conf    {@link Configuration} without self info
     * @return join success
     */
    void registerSelfToCluster(String groupId, PeerId selfIp, Configuration conf) {
        for (; ; ) {
            try {
                List<PeerId> peerIds = cliService.getPeers(groupId, conf);
                if (peerIds.contains(selfIp)) {
                    return;
                }
                Status status = cliService.addPeer(groupId, conf, selfIp);
                if (status.isOk()) {
                    return;
                }
                Loggers.RAFT.warn("Failed to join the cluster, retry...");
            } catch (Exception e) {
                Loggers.RAFT.error("Failed to join the cluster, retry...", e);
            }
            ThreadUtils.sleep(1_000L);
        }
    }
    
    protected PeerId getLeader(final String raftGroupId) {
        return RouteTable.getInstance().selectLeader(raftGroupId);
    }
    
    synchronized void shutdown() {
        if (isShutdown) {
            return;
        }
        isShutdown = true;
        try {
            Loggers.RAFT.info("========= The raft protocol is starting to close =========");
            
            for (Map.Entry<String, RaftGroupTuple> entry : multiRaftGroup.entrySet()) {
                final RaftGroupTuple tuple = entry.getValue();
                final Node node = tuple.getNode();
                tuple.node.shutdown();
                tuple.raftGroupService.shutdown();
            }
            
            cliService.shutdown();
            cliClientService.shutdown();
            
            Loggers.RAFT.info("========= The raft protocol has been closed =========");
        } catch (Throwable t) {
            Loggers.RAFT.error("There was an error in the raft protocol shutdown, cause: ", t);
        }
    }

    //node的apply操作，这是RAFT的基本上第一个操作。
    public void applyOperation(Node node, Message data, FailoverClosure closure) {
        final Task task = new Task();
        task.setDone(new NacosClosure(data, status -> {
            NacosClosure.NacosStatus nacosStatus = (NacosClosure.NacosStatus) status;
            closure.setThrowable(nacosStatus.getThrowable());
            closure.setResponse(nacosStatus.getResponse());
            closure.run(nacosStatus);
        }));
        task.setData(ByteBuffer.wrap(data.toByteArray()));
        node.apply(task);
    }

    //调用leader
    private void invokeToLeader(final String group, final Message request, final int timeoutMillis,
            FailoverClosure closure) {
        try {
            final Endpoint leaderIp = Optional.ofNullable(getLeader(group))
                    .orElseThrow(() -> new NoLeaderException(group)).getEndpoint();
            cliClientService.getRpcClient().invokeAsync(leaderIp, request, new InvokeCallback() {
                @Override
                public void complete(Object o, Throwable ex) {
                    //异常处理
                    if (Objects.nonNull(ex)) {
                        closure.setThrowable(ex);
                        closure.run(new Status(RaftError.UNKNOWN, ex.getMessage()));
                        return;
                    }
                    if (!((Response)o).getSuccess()) {
                        closure.setThrowable(new IllegalStateException(((Response) o).getErrMsg()));
                        closure.run(new Status(RaftError.UNKNOWN, ((Response) o).getErrMsg()));
                        return;
                    }
                    closure.setResponse((Response) o);
                    closure.run(Status.OK());
                }
                
                @Override
                public Executor executor() {
                    return RaftExecutor.getRaftCliServiceExecutor();
                }
            }, timeoutMillis);
        } catch (Exception e) {
            closure.setThrowable(e);
            closure.run(new Status(RaftError.UNKNOWN, e.toString()));
        }
    }
    //为什么只处理删除?
    boolean peerChange(JRaftMaintainService maintainService, Set<String> newPeers) {
        // This is only dealing with node deletion, the Raft protocol, where the node adds itself to the cluster when it starts up
        Set<String> oldPeers = new HashSet<>(this.raftConfig.getMembers());
        this.raftConfig.setMembers(localPeerId.toString(), newPeers);
        oldPeers.removeAll(newPeers);
        if (oldPeers.isEmpty()) {
            return true;
        }
        
        Set<String> waitRemove = oldPeers;
        AtomicInteger successCnt = new AtomicInteger(0);
        multiRaftGroup.forEach(new BiConsumer<String, RaftGroupTuple>() {
            @Override
            public void accept(String group, RaftGroupTuple tuple) {
                Map<String, String> params = new HashMap<>();
                params.put(JRaftConstants.GROUP_ID, group);
                params.put(JRaftConstants.COMMAND_NAME, JRaftConstants.REMOVE_PEERS);
                params.put(JRaftConstants.COMMAND_VALUE, StringUtils.join(waitRemove, StringUtils.COMMA));
                RestResult<String> result = maintainService.execute(params);
                if (result.ok()) {
                    successCnt.incrementAndGet();
                } else {
                    Loggers.RAFT.error("Node removal failed : {}", result);
                }
            }
        });
        return successCnt.get() == multiRaftGroup.size();
    }

    //刷新
    void refreshRouteTable(String group) {
        if (isShutdown) {
            return;
        }
        
        final String groupName = group;
        Status status = null;
        try {
            RouteTable instance = RouteTable.getInstance();
            Configuration oldConf = instance.getConfiguration(groupName);
            String oldLeader = Optional.ofNullable(instance.selectLeader(groupName)).orElse(PeerId.emptyPeer())
                    .getEndpoint().toString();//从缓存中拿到leader
            // fix issue #3661  https://github.com/alibaba/nacos/issues/3661
            status = instance.refreshLeader(this.cliClientService, groupName, rpcRequestTimeoutMs);
            if (!status.isOk()) {
                Loggers.RAFT.error("Fail to refresh leader for group : {}, status is : {}", groupName, status);
            }

            //刷新configuration
            status = instance.refreshConfiguration(this.cliClientService, groupName, rpcRequestTimeoutMs);
            if (!status.isOk()) {
                Loggers.RAFT
                        .error("Fail to refresh route configuration for group : {}, status is : {}", groupName, status);
            }
        } catch (Exception e) {
            Loggers.RAFT.error("Fail to refresh raft metadata info for group : {}, error is : {}", groupName, e);
        }
    }
    
    public RaftGroupTuple findTupleByGroup(final String group) {
        RaftGroupTuple tuple = multiRaftGroup.get(group);
        return tuple;
    }
    
    public Node findNodeByGroup(final String group) {
        final RaftGroupTuple tuple = multiRaftGroup.get(group);
        if (Objects.nonNull(tuple)) {
            return tuple.node;
        }
        return null;
    }
    
    Map<String, RaftGroupTuple> getMultiRaftGroup() {
        return multiRaftGroup;
    }
    
    @JustForTest
    void mockMultiRaftGroup(Map<String, RaftGroupTuple> map) {
        this.multiRaftGroup = map;
    }
    
    CliService getCliService() {
        return cliService;
    }
    
    public static class RaftGroupTuple {

        //处理器
        private RequestProcessor processor;

        //节点
        private Node node;

        //组服务
        private RaftGroupService raftGroupService;

        //复制状态机
        private NacosStateMachine machine;
        
        @JustForTest
        public RaftGroupTuple() {
        }
        
        public RaftGroupTuple(Node node, RequestProcessor processor, RaftGroupService raftGroupService,
                NacosStateMachine machine) {
            this.node = node;
            this.processor = processor;
            this.raftGroupService = raftGroupService;
            this.machine = machine;
        }
        
        public Node getNode() {
            return node;
        }
        
        public RequestProcessor getProcessor() {
            return processor;
        }
        
        public RaftGroupService getRaftGroupService() {
            return raftGroupService;
        }
    }
    
}
