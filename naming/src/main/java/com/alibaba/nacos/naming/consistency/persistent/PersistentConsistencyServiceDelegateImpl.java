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

package com.alibaba.nacos.naming.consistency.persistent;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.core.distributed.ProtocolManager;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.persistent.impl.BasePersistentServiceProcessor;
import com.alibaba.nacos.naming.consistency.persistent.impl.PersistentServiceProcessor;
import com.alibaba.nacos.naming.consistency.persistent.impl.StandalonePersistentServiceProcessor;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftConsistencyServiceImpl;
import com.alibaba.nacos.naming.pojo.Record;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Persistent consistency service delegate.
 *
 * @author xiweng.yy
 */
@Component("persistentConsistencyServiceDelegate")
public class PersistentConsistencyServiceDelegateImpl implements PersistentConsistencyService {
    
    private final ClusterVersionJudgement versionJudgement;
    
    private final RaftConsistencyServiceImpl oldPersistentConsistencyService;
    
    private final BasePersistentServiceProcessor newPersistentConsistencyService;
    
    private volatile boolean switchNewPersistentService = false;
    
    public PersistentConsistencyServiceDelegateImpl(ClusterVersionJudgement versionJudgement,
            RaftConsistencyServiceImpl oldPersistentConsistencyService, ProtocolManager protocolManager)
            throws Exception {
        this.versionJudgement = versionJudgement;
        //这里的老的持久化一致性服务，使用的老的RAFT算法。代码比较挫逼，但是容易看懂。。
        this.oldPersistentConsistencyService = oldPersistentConsistencyService;
        this.newPersistentConsistencyService = createNewPersistentServiceProcessor(protocolManager, versionJudgement);
        init();
    }
    
    private void init() {
        //是否切换新的持久层一致性服务
        this.versionJudgement.registerObserver(isAllNewVersion -> switchNewPersistentService = isAllNewVersion, -1);
    }
    
    @Override
    public void put(String key, Record value) throws NacosException {
        switchOne().put(key, value);
    }
    
    @Override
    public void remove(String key) throws NacosException {
        switchOne().remove(key);
    }
    
    @Override
    public Datum get(String key) throws NacosException {
        return switchOne().get(key);
    }
    
    @Override
    public void listen(String key, RecordListener listener) throws NacosException {
        oldPersistentConsistencyService.listen(key, listener);
        newPersistentConsistencyService.listen(key, listener);
    }
    
    @Override
    public void unListen(String key, RecordListener listener) throws NacosException {
        newPersistentConsistencyService.unListen(key, listener);
        oldPersistentConsistencyService.unListen(key, listener);
    }
    
    @Override
    public boolean isAvailable() {
        return switchOne().isAvailable();
    }
    
    @Override
    public Optional<String> getErrorMsg() {
        return switchOne().getErrorMsg();
    }
    
    private PersistentConsistencyService switchOne() {
        return switchNewPersistentService ? newPersistentConsistencyService : oldPersistentConsistencyService;
    }
    
    private BasePersistentServiceProcessor createNewPersistentServiceProcessor(ProtocolManager protocolManager,
            ClusterVersionJudgement versionJudgement) throws Exception {
        //通过 createNewPersistentServiceProcessor 方法,关联拿到CP模型的raft一致性算法框架
        //单例模式情况下，使用自己的kv模型，集群模式下，使用的是Raft
        final BasePersistentServiceProcessor processor =
                EnvUtil.getStandaloneMode() ? new StandalonePersistentServiceProcessor(versionJudgement)
                        : new PersistentServiceProcessor(protocolManager, versionJudgement);
        processor.afterConstruct();
        return processor;
    }
}
