//package com.boulderai.metabase.sync.core.service.wal;
//
//import org.springframework.amqp.core.AcknowledgeMode;
//import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
//import org.springframework.amqp.rabbit.connection.ConnectionFactory;
//import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
///**
// * @author df.l
// * @ClassName: RabbitmqConfig
// * @Description: 多线程配置，如果需要
// * @date 2022年10月17日
// * @Copyright boulderaitech.com
// */
//@Configuration
//public class RabbitmqConnetionIniter {
//    @Bean("customModelDataContainerFactory")
//    public SimpleRabbitListenerContainerFactory containerModelDataFactory(SimpleRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory) {
//        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
//        factory.setConcurrentConsumers(10); //设置线程数
//        factory.setMaxConcurrentConsumers(10); //最大线程数
//
//        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
//        //每次处理数据数量，提高并发量
//        factory.setConcurrentConsumers(1);
//        factory.setPrefetchCount(1);
//
//        configurer.configure(factory, connectionFactory);
//        return factory;
//
//    }
//
//    @Bean("customMetabaseContainerFactory")
//    public SimpleRabbitListenerContainerFactory containerMetabaseFactory(SimpleRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory) {
//        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
//        factory.setConcurrentConsumers(10); //设置线程数
//        factory.setMaxConcurrentConsumers(10); //最大线程数
//
//        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
//        //每次处理数据数量，提高并发量
//        factory.setConcurrentConsumers(1);
//        factory.setPrefetchCount(1);
//
//        configurer.configure(factory, connectionFactory);
//        return factory;
//
//    }
//
//}
