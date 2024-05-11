package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(Integer.MAX_VALUE-600)
public class MqClusterDataListenerConsumer extends MqModelDataListenerConsumer{

    public MqClusterDataListenerConsumer()
    {
        this.baseCount=150;
        this.batchUpdateData=true;
    }

    @Override
    public  ConsumerType  getConsumerType()
    {
        return ConsumerType.cluster_modeldata;
    }

}
