package com.boulderai.metabase.etl.tl.neo4j.service.wal;


import com.rabbitmq.client.Channel;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.core.annotation.Order;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@Order(Integer.MAX_VALUE-600)
public class MqIntegrationModelDataListenerConsumer extends MqModelDataListenerConsumer{


    public MqIntegrationModelDataListenerConsumer()
    {
        this.baseCount=150;
        this.batchUpdateData=true;
    }

    /**
     * 消费模型元数据队列信息建立模型节点记忆关系
     * @param    data
     * @param   deliveryTag
     * @param   channel
     * autoDelete = "true"
     */
//    @RabbitListener(bindings = @QueueBinding(value = @Queue(value = DataSynConstant.MQ_INTEGRATION_DATA_QUEUE, durable = "true"),
//            exchange = @Exchange(value = DataSynConstant.MQ_EXCHANGE_INTEGRATION_DATA_VALUE, type = ExchangeTypes.FANOUT), key = DataSynConstant.MQ_ROUTING_INTEGRATION_DATA_KEY), ackMode = "MANUAL"
//            ,concurrency =  "1"
//    )
    public void consumerPgWalChange(String data, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag, Channel channel)
    {
        super.consumerPgWalChange(data, deliveryTag,  channel,1);
    }


    @Override
    public  ConsumerType  getConsumerType()
    {
        return ConsumerType.integration_modeldata;
    }


}
