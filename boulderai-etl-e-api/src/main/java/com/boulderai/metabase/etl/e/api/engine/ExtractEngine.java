package com.boulderai.metabase.etl.e.api.engine;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

import java.util.function.Consumer;

/**
 * @author wanganbang
 */
public interface ExtractEngine {

    void closeEngine();
    void create();
}
