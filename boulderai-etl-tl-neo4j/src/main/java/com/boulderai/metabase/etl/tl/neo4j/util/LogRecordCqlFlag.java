package com.boulderai.metabase.etl.tl.neo4j.util;

import java.util.concurrent.atomic.AtomicBoolean;

public class LogRecordCqlFlag {
    private static AtomicBoolean signal = new AtomicBoolean(false);


    public static final boolean isStart() {
        return signal.get();
    }

    public static final void stop() {
        signal.set(false);
    }
    public static final void restart() {
        signal.set(true);
    }
}
