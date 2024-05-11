package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.etl.tl.neo4j.service.pipeline.DefaultMqProcessPipeline;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.etl.tl.neo4j.util.StartFlagger;
import com.boulderai.metabase.etl.tl.neo4j.util.Stopper;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName: MqMetabaseListenerConsumer
 * @Description: rabbitmq消费客户端监听器,元数据处理
 * @author  df.l
 * @date 2022年10月27日
 * @Copyright boulderaitech.com
 */
@Component
@Order(Integer.MAX_VALUE-100)
public class MqMetabaseListenerConsumer  extends   MqBaseListenerConsumer{
    protected   final static Logger logger = LoggerFactory.getLogger(MqMetabaseListenerConsumer.class);


    @Autowired
    private Neo4jConfig neo4jConfig;

    @Autowired
    private  PgWalConfig pgWalConfig;

    /**
     * Gson实例在调用Json操作时不保持任何状态,可以共享
     */
    private Gson gson = new Gson();

//    private final LinkedBlockingQueue<MqMetabase2Neo4jProcessPipeline>  pipelineQueue=new LinkedBlockingQueue<MqMetabase2Neo4jProcessPipeline>(100);

    @Override
    public  ConsumerType  getConsumerType()
    {
        return ConsumerType.metabase;
    }

    /**
     * 启动mq处理器流水线
     */
    public   void start()
    {
      this.startWith( neo4jConfig,  pgWalConfig);
    }

    /**
     * 消费模型元数据队列信息建立模型节点记忆关系
     * @param data
     * @param deliveryTag
     * @param channel
     * autoDelete = "true")
     */
    // , arguments = @Argument(name = "x-single-active-consumer", value = "true", type = "Boolean")
    @RabbitListener(bindings = @QueueBinding(value = @Queue(),
            exchange = @Exchange(value = DataSynConstant.MQ_EXCHANGE_VALUE, type = ExchangeTypes.FANOUT), key = DataSynConstant.MQ_ROUTING_KEY), ackMode = "MANUAL"
//           ,containerFactory="batchQueueRabbitListenerContainerFactory"
             ,concurrency =  "4"
          )
    @RabbitHandler
    public void consumerPgWalChange(String data, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag, Channel channel)
    {
        //其他组件没有准备好，请稍等
        if(!Stopper.isRunning()|| !StartFlagger.isOK())
        {
            onMsgAck(false, data,   deliveryTag,  channel);
            SleepUtil.sleepSecond(10);
            return  ;
        }

         if(StringUtils.isBlank(data))
         {
             onMsgAck(true, data,   deliveryTag,  channel);
            return  ;
         }

         //空转模式快速消费消息
         if(MSG_DEBUG_MANAGER.getEmptyConsumeMetaData())
         {
             onMsgAck(true, data,   deliveryTag,  channel);
             return  ;
         }


         //转换数据
        PgWalChange record =null;
        try {
            record = gson.fromJson(data, PgWalChange.class);
        }
        catch(Exception ex)
        {
            logger.error("gson fromJson error!",ex);
        }
        if(record==null)
        {
            onMsgAck(true, data,   deliveryTag,  channel);
            return  ;
        }

        PgWalChangeAck pgWalChangeAck=new PgWalChangeAck( record,  deliveryTag,  channel);

        //从队列中取出流水线开始处理数据
        DefaultMqProcessPipeline pipeline=null;
        try {
            pipeline= pipelineQueue.poll(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            logger.error("pipelineQueue  poll error!",ex);
        }
        if(pipeline!=null)
        {
            logger.info("consumer receive meta data : " + data);
            pipeline.addChange(pgWalChangeAck);
            try {
                pipeline.handleDataList();
                onMsgAck(true, data,   deliveryTag,  channel);
            }
            catch (Exception ex)
            {
                logger.error("handle change data error!",ex);
                onMsgAck(false, data,   deliveryTag,  channel);
            }
            finally {
                pipeline.clearChange();
                pipelineQueue.add(pipeline);
            }

        }
        else {
            logger.error("pipeline  queue is full error!");
            onMsgAck(false, data,   deliveryTag,  channel);
        }

    }

    public void setNeo4jConfig(Neo4jConfig neo4jConfig) {
        this.neo4jConfig = neo4jConfig;
    }

    public void setPgWalConfig(PgWalConfig pgWalConfig) {
        this.pgWalConfig = pgWalConfig;
    }
}
