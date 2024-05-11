//package com.boulderai.metabase.sync.core.service.wal;
//
//import com.boulderai.metabase.sync.core.util.DataSynConstant;
//import com.rabbitmq.client.Channel;
//import org.springframework.amqp.core.ExchangeTypes;
//import org.springframework.amqp.rabbit.annotation.Exchange;
//import org.springframework.amqp.rabbit.annotation.Queue;
//import org.springframework.amqp.rabbit.annotation.QueueBinding;
//import org.springframework.amqp.rabbit.annotation.RabbitListener;
//import org.springframework.amqp.support.AmqpHeaders;
//import org.springframework.core.annotation.Order;
//import org.springframework.messaging.handler.annotation.Header;
//import org.springframework.stereotype.Component;
//
//import java.io.IOException;
//
//@Component
//@Order(Integer.MAX_VALUE-1000)
//public class MqPerformanceListenerConsumer {
//
//    @RabbitListener(bindings = @QueueBinding(value = @Queue(value = DataSynConstant.MQ_OTHER_TEST_QUEUE, durable = "true", autoDelete = "true"),
//            exchange = @Exchange(value = DataSynConstant.MQ_EXCHANGE_OTHER_TEST_VALUE, type = ExchangeTypes.FANOUT), key = DataSynConstant.MQ_ROUTING_OTHER_TEST_KEY), ackMode = "MANUAL"
//    )
//    public void consumerPgWalChange(String data, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag, Channel channel)
//    {
//        System.out.println("Performance test data! ===> "+data);
//        try {
//            channel.basicAck(deliveryTag, false);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
