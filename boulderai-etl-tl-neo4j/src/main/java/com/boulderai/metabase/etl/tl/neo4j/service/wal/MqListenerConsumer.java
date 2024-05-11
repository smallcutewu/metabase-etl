//package com.boulderai.metabase.sync.core.service.wal;
//
//import com.boulderai.metabase.sync.core.config.Neo4jConfig;
//import com.boulderai.metabase.sync.core.config.PgWalConfig;
//import com.boulderai.metabase.sync.core.service.neo4j.Neo4jDataRepositoryContext;
//import com.boulderai.metabase.sync.core.service.pipeline.Mq2Neo4jProcessPipeline;
//import com.boulderai.metabase.sync.core.service.wal.model.PgWalChange;
//import com.boulderai.metabase.sync.core.util.DataSynConstant;
//import com.boulderai.metabase.sync.core.util.StartFlagger;
//import com.boulderai.metabase.sync.core.util.Stopper;
//import com.google.gson.Gson;
//import com.rabbitmq.client.Channel;
//import org.apache.commons.lang3.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.amqp.core.ExchangeTypes;
//import org.springframework.amqp.rabbit.annotation.Exchange;
//import org.springframework.amqp.rabbit.annotation.Queue;
//import org.springframework.amqp.rabbit.annotation.QueueBinding;
//import org.springframework.amqp.rabbit.annotation.RabbitListener;
//import org.springframework.amqp.support.AmqpHeaders;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.core.annotation.Order;
//import org.springframework.messaging.handler.annotation.Header;
//import org.springframework.stereotype.Component;
//
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.TimeUnit;
//
///**
// * @ClassName: MqListenerConsumer
// * @Description: rabbitmq消费客户端监听器
// * @author  df.l
// * @date 2022年10月17日
// * @Copyright boulderaitech.com
// */
////@Component
////@Order(Integer.MAX_VALUE)
//public class MqListenerConsumer {
//
//    protected   final static Logger logger = LoggerFactory.getLogger(MqListenerConsumer.class);
//
//    @Autowired
//    private Neo4jConfig neo4jConfig;
//
//    @Autowired
//    private  PgWalConfig pgWalConfig;
//
//    /**
//     * Gson实例在调用Json操作时不保持任何状态,可以共享
//     */
//    private Gson gson = new Gson();
//
//    private final LinkedBlockingQueue<Mq2Neo4jProcessPipeline>  pipelineQueue=new LinkedBlockingQueue<Mq2Neo4jProcessPipeline>();
//
//
//    /**
//     * 启动mq处理器流水线
//     */
//    public   void start()
//    {
//        int pipeWorkCount=pgWalConfig.getPipeWorkCount();
//        for (int i = 0; i < pipeWorkCount; i++) {
//            Mq2Neo4jProcessPipeline pipeline=new Mq2Neo4jProcessPipeline(i);
//            pipeline.setNeo4jConfig(neo4jConfig);
//            pipeline.addHandlers();
//            Neo4jDataRepositoryContext.init(neo4jConfig,i);
//            pipelineQueue.add(pipeline);
//        }
//
//    }
//
////    /**
////     * 消费模型元数据队列信息建立模型节点记忆关系
////     * @param data
////     * @param deliveryTag
////     * @param channel
////     */
////    @RabbitListener(bindings = @QueueBinding(value = @Queue(value = DataSynConstant.MQ_QUEUE, durable = "false", autoDelete = "true"),
////            exchange = @Exchange(value = DataSynConstant.MQ_EXCHANGE_VALUE, type = ExchangeTypes.FANOUT), key = DataSynConstant.MQ_ROUTING_KEY), ackMode = "MANUAL"
//////           ,containerFactory="customContainerFactory"
////          )
////    public void consumerPgWalChange(String data, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag, Channel channel)
////    {
////        //其他组件没有准备好，请稍等
////        if(!Stopper.isRunning()|| !StartFlagger.isOK())
////        {
////            return  ;
////        }
////
////         if(StringUtils.isBlank(data))
////         {
////             onMsgAck(true, data,   deliveryTag,  channel);
////            return  ;
////         }
////         logger.info("consumer receive data : " + data);
////
////         //转换数据
////        PgWalChange record =null;
////        try {
////            record = gson.fromJson(data, PgWalChange.class);
////        }
////        catch(Exception ex)
////        {
////            logger.error("gson fromJson error!",ex);
////        }
////        if(record==null)
////        {
////            onMsgAck(true, data,   deliveryTag,  channel);
////            return  ;
////        }
////
////        //从队列中取出流水线开始处理数据
////        Mq2Neo4jProcessPipeline pipeline=null;
////        try {
////            pipeline= pipelineQueue.poll(20, TimeUnit.SECONDS);
////        } catch (Exception ex) {
////            logger.error("pipelineQueue  poll error!",ex);
////        }
////        if(pipeline!=null)
////        {
////            pipeline.addChange(record);
////            try {
////                pipeline.handleDataList();
////                onMsgAck(true, data,   deliveryTag,  channel);
////            }
////            catch (Exception ex)
////            {
////                logger.error("handle change data error!",ex);
////                onMsgAck(false, data,   deliveryTag,  channel);
////            }
////            finally {
////                pipeline.clearChange();
////                pipelineQueue.add(pipeline);
////            }
////
////        }
////        else {
////            logger.error("pipeline  queue is full error!");
////            onMsgAck(true, data,   deliveryTag,  channel);
////        }
////
////    }
//
//    /**
//     * 消费数据响应ack给rabbitmq
//     * @param success 是否消费成功
//     * @param data 数据内容
//     * @param deliveryTag
//     * @param channel
//     */
//    private void onMsgAck(Boolean success,String data,  long deliveryTag, Channel channel)
//    {
//        try{
//            if (success) {
//                // RabbitMQ的ack机制中，第二个参数返回true，表示需要将这条消息投递给其他的消费者重新消费
//                channel.basicAck(deliveryTag, false);
//            } else {
//                // 第三个参数true，表示这个消息会重新进入队列
//                channel.basicNack(deliveryTag, false, true);
//            }
//        }
//        catch (Exception ex)
//        {
//            logger.error(" ack mq data error!data=>>： "+data,ex) ;
//        }
//
//    }
//}
