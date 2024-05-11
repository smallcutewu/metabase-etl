//package com.boulderai.metabase.sync.core.service.pipeline;
//
//import com.boulderai.metabase.sync.core.service.handler.LastDataHandler;
//import com.boulderai.metabase.sync.core.service.handler.neo4j.*;
//
///**
// * @ClassName: Neo4jProcessPipeline
// * @Description: neo4j 的Pipeline实现
// * @author  df.l
// * @date 2022年10月11日
// * @Copyright boulderaitech.com
// */
//public class Neo4jProcessPipeline   extends  DefaultProcessPipeline{
//
//    public Neo4jProcessPipeline(int  pipeId)
//    {
//        super(pipeId);
//    }
//
//    @Override
//    public void addHandlers() {
//        super.addHandlers();
//        this.addLast(new TransformEventDataHandler());
////        this.addLast(new PersistDataHandler());
//        this.addLast(new PersistMetabaseDataHandler());
////        this.addLast(new WalRecordRelationDataHandler());
//        this.addLast(new WalRecordMetabaseRelationDataHandler());
//        this.addLast(new LastDataHandler());
//
//    }
//}
