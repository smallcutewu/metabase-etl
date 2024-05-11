package com.boulderai.metabase.etl.tl.neo4j.service.pipeline;

import com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.TransformEventDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.model.PersistModelDataDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.model.WalRecordModelDataRelationHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.LastDataHandler;

/**
 * @ClassName: MqMetabase2Neo4jProcessPipeline
 * @Description: mq消息处理pipeline，处理真实数据model
 * @author  df.l
 * @date 2022年10月24日
 * @Copyright boulderaitech.com
 */
public class MqModelData2Neo4jProcessPipeline extends  DefaultMqProcessPipeline{


    public MqModelData2Neo4jProcessPipeline(int  pipeId)
    {
        super(pipeId);
    }

    @Override
    public void addHandlers() {
        super.addHandlers();
        this.addLast(new TransformEventDataHandler());
        this.addLast(new PersistModelDataDataHandler());
        this.addLast(new WalRecordModelDataRelationHandler());
        this.addLast(new LastDataHandler());
    }
}
