package com.boulderai.metabase.etl.tl.neo4j.util;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class IntegrationDataStopper {
    private static AtomicBoolean signal = new AtomicBoolean(true);


    public static final boolean isStart() {
        return signal.get();
    }

    public static final void stop() {
        signal.set(false);
        log.info("开启集成数据本地队列消息空转直接丢弃......");
    }
    public static final void restart() {
        signal.set(true);
        log.info("关闭集成数据本地队列消息空转直接丢弃......");
    }
}
