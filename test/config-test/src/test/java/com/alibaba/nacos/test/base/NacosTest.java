package com.alibaba.nacos.test.base;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.Event;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Cluster;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.Service;
import com.alibaba.nacos.api.naming.pojo.healthcheck.AbstractHealthChecker;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.junit.Test;

/**
 * @author ligm
 * @version 1.0
 * @date 2022/2/20 19:25
 */
public class NacosTest {

    @Test
    public void testConfig() throws Exception {
        String serverAddr = "127.0.0.1:8848";
        String dataId = "example";
        String group = "example";
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, serverAddr);
        ConfigService configService = NacosFactory.createConfigService(properties);
        String content = configService.getConfig(dataId, group, 50000);
        System.out.println(content);
        configService.addListener(dataId, group, new Listener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                System.out.println("recieve1:" + configInfo);
            }

            @Override
            public Executor getExecutor() {
                return null;
            }
        });
        boolean isPublishOk = configService
            .publishConfig(dataId, group, "appSecret=xtxwrwrw345466sfsfsf");
        System.out.println(isPublishOk);

        Thread.sleep(3000);
        content = configService.getConfig(dataId, group, 5000);
        System.out.println(content);

        boolean isRemoveOk = configService.removeConfig(dataId, group);
        System.out.println(isRemoveOk);
        Thread.sleep(3000);

        content = configService.getConfig(dataId, group, 5000);
        System.out.println(content);
        Thread.sleep(300000);

//         测试让主线程不退出，因为订阅配置是守护线程，主线程退出守护线程就会退出。 正式代码中无需下面代码
//        while (true) {
//            try {
//                Thread.sleep(5000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
    }

    @Test
    public void testRegisterInstance() throws Exception {
        NamingService naming = NamingFactory.createNamingService("127.0.0.1:8848");
        naming.registerInstance("cn.spring.nacos.example", "NACOS_EXAMPLE", "11.11.11.11", 8888,
            "TEST1");

        naming.registerInstance("cn.spring.nacos.example", "NACOS_EXAMPLE", "131.131.131.131", 8888,
            "TEST1");

        Instance instance = new Instance();
        instance.setIp("111.111.111.111");
        instance.setInstanceId("122.122.122.122");
        instance.setPort(999);
        instance.setClusterName("CLUSTER1");
        instance.setEnabled(Boolean.TRUE);
        instance.setEphemeral(true);
        instance.setWeight(2);
        instance.setHealthy(false);
        Map<String, String> meta = new HashMap<>();
        meta.put("site", "ec2");
        instance.setMetadata(meta);
        instance.setServiceName("service");
        naming.registerInstance("cn.spring.nacos.instant", instance);
        naming.subscribe("cn.spring.nacos.instant", event -> {
            if (event instanceof NamingEvent) {
                System.out.println(((NamingEvent) event).getInstances());
            }
        });

        List<Instance> list = naming.getAllInstances("cn.spring.nacos.example");
        list.forEach(System.out::print);
    }

    @Test
    public void testDeregisterInstance() throws Exception {
        NamingService namingService = NacosFactory.createNamingService("127.0.0.1:8848");
        namingService.deregisterInstance("service", "122.122.122.122", 999);
    }
}
