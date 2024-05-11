package com.boulderai.metabase.etl.tl.neo4j.util;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @ClassName: Stopper
 * @Description: wal多线程消费者停止标志
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public class ClearingNeo4jStopper {

    private static AtomicBoolean signal = new AtomicBoolean(false);

    public static final boolean isStopped() {
        return signal.get();
    }

    public static final boolean isRunning() {
        return !signal.get();
    }

    public static final void stop() {
        signal.set(true);
    }
    public static final void restart() {
        signal.set(false);
    }
}