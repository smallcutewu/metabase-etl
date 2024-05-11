package com.boulderai.metabase.etl.tl.neo4j.util;

import com.alibaba.ttl.TransmittableThreadLocal;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.DataSynTopics;

public class HandleDataEnv {
    private  final  static TransmittableThreadLocal<Long> LASTOPER_TIMEMAP = new TransmittableThreadLocal<Long>();

    private final  static TransmittableThreadLocal<DataSynTopics> THREAD_DATA_SYN_TOPICS = new TransmittableThreadLocal<DataSynTopics>();


    public static void addLastHandleTime() {
        LASTOPER_TIMEMAP.set(System.currentTimeMillis());
    }

    public static Long getLastHandleTime() {
        return LASTOPER_TIMEMAP.get();
    }

    public static void addDataSynTopics(DataSynTopics  topic) {
        THREAD_DATA_SYN_TOPICS.set(topic);
    }

    public static DataSynTopics getDataSynTopicse() {
        return THREAD_DATA_SYN_TOPICS.get();
    }
}
