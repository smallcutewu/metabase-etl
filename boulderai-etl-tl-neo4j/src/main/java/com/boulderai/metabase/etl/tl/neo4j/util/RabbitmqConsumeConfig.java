//package com.boulderai.metabase.sync.core.util;
//
//import org.springframework.amqp.core.AcknowledgeMode;
//import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
//import org.springframework.amqp.rabbit.connection.ConnectionFactory;
//import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
//import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.Primary;
//import org.springframework.core.task.TaskExecutor;
//import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
//
///**
// * @ClassName: RabbitmqConsumeConfig
// * @Description: rabbitmq多线程消费
// * @author  df.l
// * @date 2022年12月02日
// * @Copyright boulderaitech.com
// */
//@Configuration
//public class RabbitmqConsumeConfig {
//
//    @Bean("batchQueueRabbitListenerContainerFactory")
//    public RabbitListenerContainerFactory<?> rabbitListenerContainerFactory(ConnectionFactory connectionFactory){
//        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
//        factory.setConnectionFactory(connectionFactory);
////        factory.setMessageConverter(new Jackson2JsonMessageConverter());
//        //确认方式,manual为手动ack.
//        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
//        //每次处理数据数量，提高并发量
//        factory.setPrefetchCount(3);
//        //设置线程数
//        factory.setConcurrentConsumers(10);
//        //最大线程数
//        factory.setMaxConcurrentConsumers(20);
//        /* setConnectionFactory：设置spring-amqp的ConnectionFactory。 */
//        factory.setConnectionFactory(connectionFactory);
////        factory.setConcurrentConsumers(1);
////        factory.setPrefetchCount(1);
//        factory.setDefaultRequeueRejected(true);
//        //使用自定义线程池来启动消费者。
//        factory.setTaskExecutor(taskExecutor());
//        return factory;
//    }
//
//    @Bean("metabaseTaskExecutor")
//    @Primary
//    public TaskExecutor taskExecutor() {
//        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
//        // 设置核心线程数
//        executor.setCorePoolSize(10);
//        // 设置最大线程数
//        executor.setMaxPoolSize(30);
//        // 设置队列容量
//        executor.setQueueCapacity(0);
//        // 设置线程活跃时间（秒）
//        executor.setKeepAliveSeconds(300);
//        // 设置默认线程名称
//        executor.setThreadNamePrefix("thread-file-queue");
//        // 设置拒绝策略rejection-policy：当pool已经达到max size的时候，丢弃
//        // executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
//        // 等待所有任务结束后再关闭线程池
//        executor.setWaitForTasksToCompleteOnShutdown(true);
//        return executor;
//    }
//}