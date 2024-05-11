package com.boulderai.metabase.etl.starter.electMasterService;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.notify.Event;
import com.boulderai.metabase.etl.starter.electMasterService.util.NetUtil;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.lang.util.SleepUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 服务变更监听
 *
 * @author butterfly
 * @date 2023-09-24
 */
@Slf4j
@Component
public class nacosCallback extends Subscriber<InstancesChangeEvent> {

    private final Stack<List> historyEvent = new Stack<>();

    private final Lock lock = new ReentrantLock();

    public static ArrayList<String> LOCALHOST_IP_PORT;

    @Autowired
    MasterLockService commonLockService;


    @PostConstruct
    public void init() {
        // 注册当前自定义的订阅者以获取通知
        NotifyCenter.registerSubscriber(this);
    }

    @Override
    public void onEvent(InstancesChangeEvent event) {
        String serviceName = event.getServiceName();
        // 使用 dubbo 时包含 rpc 服务类会注册以 providers: 或者 consumers: 开头的服务
        if (!serviceName.contains(DataSynConstant.SYSTEM_NAME)) {
            return;
        }
        List<Instance> concurrentHost = event.getHosts();
        log.info("服务上下线: {}", JSON.toJSONString(event));
        // 和历史的比较 THISUP, THISDOWN, OTHERUP,OTHERDOWN
        lock.lock();
        String eventType = getEventType(event.getHosts());
        historyEvent.push(concurrentHost);
        switch (eventType) {
            case "THISUP" :
                log.info("当前节点上线");
                commonLockService.lock();
                break;
            case "THISDOWN" :
                log.info("当前节点下线");
                commonLockService.unlock();
                break;
            case "OTHERDOWN" :
                log.info("其余节点下线");
                SleepUtil.sleepSecond(3);
                commonLockService.lock();
                break;
            case "OTHERUP" :
                log.info("其余节点上线");
                break;
        }
        lock.unlock();
        // serviceName 格式为 groupName@@name
        String split = Constants.SERVICE_INFO_SPLITER;
        if (serviceName.contains(split)) {
            serviceName = serviceName.substring(serviceName.indexOf(split) + split.length());
        }
        // 针对服务进行后续更新操作
    }

    @Override
    public Class<? extends Event> subscribeType() {
        return InstancesChangeEvent.class;
    }

    private String getEventType(List<Instance> concurrentHost) {
        if (CollectionUtils.isEmpty(historyEvent)) {
            return "THISUP";
        }
        List<Instance> lastEvent = historyEvent.peek();
        if (concurrentHost.size()< lastEvent.size()) {
            for (Instance lainstance : lastEvent) {
                boolean lastalive = false;
                for (Instance  cuinstance : concurrentHost) {
                    if (cuinstance.getIp().equals(lainstance.getIp())) {
                        lastalive = true;
                    }
                }
                if (!lastalive) {
                    if (LOCALHOST_IP_PORT.contains(lainstance.getIp())) {
                        return "THISDOWN";
                    }
                }
            }
            return "OTHERDOWN";
        } else {
            for (Instance curinstance : concurrentHost) {
                boolean curup = true;
                for (Instance lainstance : lastEvent) {
                    if (lainstance.getIp().equals(curinstance.getIp())) {
                        curup = false;
                    }
                }
                if (curup) {
                    if (LOCALHOST_IP_PORT.contains(curinstance.getIp())) {
                        return "THISUP";
                    }
                }
            }
            return "OTHERUP";
        }
    }

    @PostConstruct
    public void initParam() {
        // 初始化参数；
        LOCALHOST_IP_PORT = NetUtil.getLocalIpAddr();
    }
}
