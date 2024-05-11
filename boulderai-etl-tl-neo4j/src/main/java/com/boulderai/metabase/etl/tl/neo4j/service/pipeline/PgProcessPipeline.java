//package com.boulderai.metabase.sync.core.service.pipeline;
//
//import com.boulderai.metabase.sync.core.service.handler.LastDataHandler;
//import com.boulderai.metabase.sync.core.service.handler.pg.PgPersistDataHandler;
//
//import java.util.Map;
//
//public class PgProcessPipeline   extends  DefaultProcessPipeline{
//
//    public PgProcessPipeline() {
//
//    }
//
//    @Override
//    protected void addHandlers() {
//        super.addHandlers();
//        this.addLast(new PgPersistDataHandler());
//        this.addLast(new LastDataHandler());
//    }
//}
