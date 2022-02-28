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

package com.alibaba.nacos.core.distributed.raft.utils;

import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.core.distributed.raft.RaftConfig;
import com.alibaba.nacos.core.distributed.raft.RaftSysConstants;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReadOnlyOption;
import com.alibaba.nacos.common.utils.StringUtils;

import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.APPLY_BATCH;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_APPLY_BATCH;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_DISRUPTOR_BUFFER_SIZE;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_ELECTION_HEARTBEAT_FACTOR;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_ENABLE_LOG_ENTRY_CHECKSUM;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_MAX_APPEND_BUFFER_SIZE;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_MAX_BODY_SIZE;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_MAX_BYTE_COUNT_PER_RPC;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_MAX_ELECTION_DELAY_MS;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_MAX_ENTRIES_SIZE;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_MAX_REPLICATOR_INFLIGHT_MSGS;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_REPLICATOR_PIPELINE;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_SYNC;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DEFAULT_SYNC_META;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.DISRUPTOR_BUFFER_SIZE;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.ELECTION_HEARTBEAT_FACTOR;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.ENABLE_LOG_ENTRY_CHECKSUM;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.MAX_APPEND_BUFFER_SIZE;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.MAX_BODY_SIZE;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.MAX_BYTE_COUNT_PER_RPC;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.MAX_ELECTION_DELAY_MS;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.MAX_ENTRIES_SIZE;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.MAX_REPLICATOR_INFLIGHT_MSGS;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.REPLICATOR_PIPELINE;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.SYNC;
import static com.alibaba.nacos.core.distributed.raft.RaftSysConstants.SYNC_META;

/**
 * build {@link RaftOptions}.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class RaftOptionsBuilder {
    
    /**
     * By {@link RaftConfig} creating a {@link RaftOptions}.
     *
     * @param config {@link RaftConfig}
     * @return {@link RaftOptions}
     */
    public static RaftOptions initRaftOptions(RaftConfig config) {
        RaftOptions raftOptions = new RaftOptions();
        raftOptions.setReadOnlyOptions(raftReadIndexType(config));//只读

        //每次RPC最大的字节数
        raftOptions.setMaxByteCountPerRpc(
                ConvertUtils.toInt(config.getVal(MAX_BYTE_COUNT_PER_RPC), DEFAULT_MAX_BYTE_COUNT_PER_RPC));

        //最大的实例数，默认1024
        raftOptions.setMaxEntriesSize(ConvertUtils.toInt(config.getVal(MAX_ENTRIES_SIZE), DEFAULT_MAX_ENTRIES_SIZE));

        //最大的body大小，默认512K
        raftOptions.setMaxBodySize(ConvertUtils.toInt(config.getVal(MAX_BODY_SIZE), DEFAULT_MAX_BODY_SIZE));

        //日志存储缓冲区，我的理解是，committed的，但是没有刷盘的这部分的缓存.默认512
        raftOptions.setMaxAppendBufferSize(
                ConvertUtils.toInt(config.getVal(MAX_APPEND_BUFFER_SIZE), DEFAULT_MAX_APPEND_BUFFER_SIZE));

        //默认延期实现，是个随机值
        raftOptions.setMaxElectionDelayMs(
                ConvertUtils.toInt(config.getVal(MAX_ELECTION_DELAY_MS), DEFAULT_MAX_ELECTION_DELAY_MS));

        //心跳：选举超时/心跳超时的比值
        raftOptions.setElectionHeartbeatFactor(
                ConvertUtils.toInt(config.getVal(ELECTION_HEARTBEAT_FACTOR), DEFAULT_ELECTION_HEARTBEAT_FACTOR));

        // 32
        raftOptions.setApplyBatch(ConvertUtils.toInt(config.getVal(APPLY_BATCH), DEFAULT_APPLY_BATCH));

        // 是否同步
        raftOptions.setSync(ConvertUtils.toBoolean(config.getVal(SYNC), DEFAULT_SYNC));
        
        raftOptions.setSyncMeta(ConvertUtils.toBoolean(config.getVal(SYNC_META), DEFAULT_SYNC_META));

        // DisruptorBuffer 大小
        raftOptions.setDisruptorBufferSize(
                ConvertUtils.toInt(config.getVal(DISRUPTOR_BUFFER_SIZE), DEFAULT_DISRUPTOR_BUFFER_SIZE));

        // 是否启用复制的 pipeline 请求优化，默认打开
        raftOptions.setReplicatorPipeline(
                ConvertUtils.toBoolean(config.getVal(REPLICATOR_PIPELINE), DEFAULT_REPLICATOR_PIPELINE));

        //求情况下，最大 in-flight请求数，默认256
        raftOptions.setMaxReplicatorInflightMsgs(
                ConvertUtils.toInt(config.getVal(MAX_REPLICATOR_INFLIGHT_MSGS), DEFAULT_MAX_REPLICATOR_INFLIGHT_MSGS));

        //是否启用 LogEntry checksum
        raftOptions.setEnableLogEntryChecksum(
                ConvertUtils.toBoolean(config.getVal(ENABLE_LOG_ENTRY_CHECKSUM), DEFAULT_ENABLE_LOG_ENTRY_CHECKSUM));
        
        return raftOptions;
    }
    
    private static ReadOnlyOption raftReadIndexType(RaftConfig config) {
        String readOnySafe = "ReadOnlySafe";
        String readOnlyLeaseBased = "ReadOnlyLeaseBased";
        
        String val = config.getVal(RaftSysConstants.RAFT_READ_INDEX_TYPE);
        
        if (StringUtils.isBlank(val) || StringUtils.equals(readOnySafe, val)) {
            return ReadOnlyOption.ReadOnlySafe;
        }
        
        if (StringUtils.equals(readOnlyLeaseBased, val)) {
            return ReadOnlyOption.ReadOnlyLeaseBased;
        }
        throw new IllegalArgumentException("Illegal Raft system parameters => ReadOnlyOption" + " : [" + val
                + "], should be 'ReadOnlySafe' or 'ReadOnlyLeaseBased'");
        
    }
    
}
