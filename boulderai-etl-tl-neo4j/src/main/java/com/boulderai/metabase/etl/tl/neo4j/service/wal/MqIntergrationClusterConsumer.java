package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.etl.tl.neo4j.util.StartFlagger;
import com.boulderai.metabase.etl.tl.neo4j.util.Stopper;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
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

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Order(Integer.MAX_VALUE-100)
public class MqIntergrationClusterConsumer extends   MqBaseListenerConsumer{
    protected   final static Logger logger = LoggerFactory.getLogger(MqIntergrationClusterConsumer.class);

    @Autowired
    private Neo4jConfig neo4jConfig;

    @Autowired
    private PgWalConfig pgWalConfig;

    /**
     * Gson实例在调用Json操作时不保持任何状态,可以共享
     */
    private Gson gson = new Gson();

    private ClusterMqStore clusterMqStore;

    @Override
    public  ConsumerType  getConsumerType()
    {
        return ConsumerType.metabase;
    }

    private AtomicInteger msgCount =new AtomicInteger(0);

    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);

    private Channel clusterChannel;

    private AtomicLong tryAgain =new AtomicLong(0);

    private final int MAXTRYTIME = 30;

    /**
     * 启动mq处理器流水线
     */
    public   void start()
    {
        this.startWith( neo4jConfig,  pgWalConfig);
        // 定时拉取队列的消息数量
        scheduledThreadPoolExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    AMQP.Queue.DeclareOk declareOk = clusterChannel.queueDeclarePassive(DataSynConstant.MQ_INTEGRATION_CLUSTER_QUEUE);
                    msgCount.set(declareOk.getMessageCount());
                    tryAgain.set(0);
                } catch (Exception e) {
                    //
                }
            }
        },1000,5000,TimeUnit.MILLISECONDS);
    }

    /**
     * 消费模型元数据队列信息建立模型节点记忆关系
     * @param data
     * @param deliveryTag
     * @param channel
     * autoDelete = "true")
     */
    @RabbitListener(bindings = @QueueBinding(value = @Queue(value = DataSynConstant.MQ_INTEGRATION_CLUSTER_QUEUE, durable = "true"),
            exchange = @Exchange(value = DataSynConstant.MQ_EXCHANGE_INTEGRATION_CLUSTER_VALUE, type = ExchangeTypes.FANOUT), key = DataSynConstant.MQ_EXCHANGE_INTEGRATION_CLUSTER_KEY), ackMode = "MANUAL"
//           ,containerFactory="batchQueueRabbitListenerContainerFactory"
            ,concurrency =  "1"
    )
    @RabbitHandler
    public void consumerPgWalChange(String data, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag, Channel channel)
    {
//        AMQP.Exchange.DeclareOk declareOk = channel.getConnection().;

        //其他组件没有准备好，请稍等
        if(!Stopper.isRunning()|| !StartFlagger.isOK())
        {
            onMsgAck(false, data,   deliveryTag,  channel);
            SleepUtil.sleepSecond(10);
            return  ;
        }
        if (clusterChannel == null) {
            clusterChannel = channel;
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

        try {
            String result =  clusterMqStore.transform(data);
            switch(result) {
                case "wait" :
                    String tryResult = "wait";
                    for (int i = 0; "wait".equals(tryResult)&&i<= MAXTRYTIME ;i++) {
                        logger.info("clusterMqStore内存队列已满，休眠5秒");
                        SleepUtil.sleepSecond(5);
                        tryResult = clusterMqStore.transform(data);
                    }
                    if ("wait".equals(tryResult) || "false".equals(tryResult)) {
                        onMsgAck(false, data,   deliveryTag,  channel);
                    } else if ("true".equals(tryResult)) {
                        onMsgAck(true, data,   deliveryTag,  channel);
                    }
                    break;
                case "true" :
                    onMsgAck(true, data,   deliveryTag,  channel);
                    int  leftNum=msgCount.decrementAndGet();
                    if (leftNum%100==0) {
                        logger.info("集群中集成数据  "+leftNum+" left model data to send !");
                    }
                    if (leftNum==0) {
                        logger.info(" in 集群中集成数据 rabbitmq queue left count is zero!");
                    }
                    break;
                case "false" :
                    onMsgAck(false, data,   deliveryTag,  channel);
                    break;
            }

        } catch (Exception e) {
            logger.error("handle change data error!",e);
            onMsgAck(false, data,   deliveryTag,  channel);
        }


//        //转换数据
//        PgWalChange record =null;
//        try {
//            record = gson.fromJson(data, PgWalChange.class);
//        }
//        catch(Exception ex)
//        {
//            logger.error("gson fromJson error!",ex);
//        }
//        if(record==null)
//        {
//            onMsgAck(true, data,   deliveryTag,  channel);
//            return  ;
//        }
//
//        PgWalChangeAck pgWalChangeAck=new PgWalChangeAck( record,  deliveryTag,  channel);
//
//        //从队列中取出流水线开始处理数据
//        DefaultMqProcessPipeline pipeline=null;
//        try {
//            pipeline= pipelineQueue.poll(10, TimeUnit.SECONDS);
//        } catch (Exception ex) {
//            logger.error("pipelineQueue  poll error!",ex);
//        }
//        if(pipeline!=null)
//        {
//            logger.info("consumer receive meta data : " + data);
//            pipeline.addChange(pgWalChangeAck);
//            try {
//                pipeline.handleDataList();
//                onMsgAck(true, data,   deliveryTag,  channel);
//            }
//            catch (Exception ex)
//            {
//                logger.error("handle change data error!",ex);
//                onMsgAck(false, data,   deliveryTag,  channel);
//            }
//            finally {
//                pipeline.clearChange();
//                pipelineQueue.add(pipeline);
//            }
//
//        }
//        else {
//            logger.error("pipeline  queue is full error!");
//            onMsgAck(false, data,   deliveryTag,  channel);
//        }

    }

    public ClusterMqStore getClusterMqStore() {
        return clusterMqStore;
    }

    public void setClusterMqStore(ClusterMqStore clusterMqStore) {
        this.clusterMqStore = clusterMqStore;
    }

    public void setNeo4jConfig(Neo4jConfig neo4jConfig) {
        this.neo4jConfig = neo4jConfig;
    }

    public void setPgWalConfig(PgWalConfig pgWalConfig) {
        this.pgWalConfig = pgWalConfig;
    }
}
