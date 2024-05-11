package com.boulderai.metabase.etl.tl.neo4j.service.wal;


import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.core.annotation.Order;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@Order(Integer.MAX_VALUE-150)
public class MqNpiModelDataListenerConsumer extends MqModelDataListenerConsumer{


    public MqNpiModelDataListenerConsumer()
    {
        this.baseCount=10;
        this.dealDataCount=10;
        this.batchUpdateData=false;
    }

    /**
     * 消费模型元数据队列信息建立模型节点记忆关系
     * @param    data
     * @param   deliveryTag
     * @param   channel
     * autoDelete = "true"
     */
    @RabbitListener(bindings = @QueueBinding(value = @Queue(value = DataSynConstant.MQ_NPI_DATA_QUEUE, durable = "true"),
            exchange = @Exchange(value = DataSynConstant.MQ_EXCHANGE_NPI_DATA_VALUE, type = ExchangeTypes.FANOUT), key = DataSynConstant.MQ_ROUTING_NPI_DATA_KEY), ackMode = "MANUAL"
            ,concurrency =  "1"
    )
    public void consumerPgWalChange(String data, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag, Channel channel)
    {
        super.consumerPgWalChange(data, deliveryTag,  channel,2);
    }


    @Override
    public  ConsumerType  getConsumerType()
    {
        return ConsumerType.api_modeldata;
    }


}
