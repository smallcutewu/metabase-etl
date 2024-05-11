package com.boulderai.metabase.etl.tl.neo4j.service.pipeline;

import com.boulderai.metabase.etl.tl.neo4j.service.handler.LastDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.meta.PersistMetabaseDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.TransformEventDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.meta.WalRecordMetabaseRelationDataHandler;

/**
 * @ClassName: MqMetabase2Neo4jProcessPipeline
 * @Description: mq消息处理pipeline,处理元数据
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public class MqMetabase2Neo4jProcessPipeline extends  DefaultMqProcessPipeline{



    public MqMetabase2Neo4jProcessPipeline(int  pipeId)
    {
        super(pipeId);
    }

    @Override
    public void addHandlers() {
        super.addHandlers();
        this.addLast(new TransformEventDataHandler());
//        this.addLast(new PersistDataHandler());
        this.addLast(new PersistMetabaseDataHandler());
//        this.addLast(new WalRecordRelationDataHandler());
        this.addLast(new WalRecordMetabaseRelationDataHandler());
        this.addLast(new LastDataHandler());

    }
}
