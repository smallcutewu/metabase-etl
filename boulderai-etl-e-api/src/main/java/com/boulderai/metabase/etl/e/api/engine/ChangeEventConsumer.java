package com.boulderai.metabase.etl.e.api.engine;


import io.debezium.engine.ChangeEvent;

/**
 * @author wanganbang
 */
public interface ChangeEventConsumer {

    boolean consumer( ChangeEvent<String, String> event );
}
