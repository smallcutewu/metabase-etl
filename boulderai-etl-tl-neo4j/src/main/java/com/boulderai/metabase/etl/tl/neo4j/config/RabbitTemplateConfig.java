//package com.boulderai.metabase.sync.core.config;
//
//import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
//import org.springframework.amqp.rabbit.connection.ConnectionFactory;
//import org.springframework.amqp.rabbit.core.RabbitTemplate;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.ComponentScan;
//import org.springframework.context.annotation.Configuration;
//
//@Configuration
//@ComponentScan("com.boulderai.metabase.sync.core.*")
//public class RabbitTemplateConfig {
//    @Bean
//    public RabbitTemplate rabbitTemplate( ) {
//        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory());
//        return rabbitTemplate;
//    }
//
//    @Bean
//     public ConnectionFactory connectionFactory() {
//                CachingConnectionFactory connection = new CachingConnectionFactory();
//                connection.setAddresses("192.168.10.110:5672");
//                 connection.setUsername("guest");
//                connection.setPassword("guest");
//                connection.setVirtualHost("/");
//                return connection;
//    }
//}
