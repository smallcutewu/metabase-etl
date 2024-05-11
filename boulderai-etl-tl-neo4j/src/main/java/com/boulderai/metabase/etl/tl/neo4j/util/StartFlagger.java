package com.boulderai.metabase.etl.tl.neo4j.util;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @ClassName: StartFlagger
 * @Description: 启动成功与否标志
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public class StartFlagger {
    private static AtomicBoolean signal = new AtomicBoolean(false);


    public static final boolean isOK() {
        return signal.get();
    }

    public static final void stop() {
        signal.set(false);
    }
    public static final void restart() {
        signal.set(true);
    }
}
