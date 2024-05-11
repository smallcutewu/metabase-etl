package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepositoryContext;
import com.boulderai.metabase.etl.tl.neo4j.service.pipeline.DefaultMqProcessPipeline;
import com.boulderai.metabase.etl.tl.neo4j.service.pipeline.MqMetabase2Neo4jProcessPipeline;
import com.boulderai.metabase.etl.tl.neo4j.service.pipeline.MqModelData2Neo4jProcessPipeline;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;
import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.RabbitConfig;
import com.boulderai.metabase.etl.tl.neo4j.util.SpringApplicationContext;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @ClassName: MqBaseListenerConsumer
 * @Description: rabbitmq消费客户端监听器基础类
 * @author  df.l
 * @date 2022年10月27日
 * @Copyright boulderaitech.com
 */
@Slf4j
public abstract class MqBaseListenerConsumer {

    protected final static MsgDebugManager   MSG_DEBUG_MANAGER=MsgDebugManager.getInstance();

    public  enum ConsumerType
    {
        /**
         * 默认
         */
        none,

        /**
         * 处理元数据
         */
        metabase  ,

        /**
         * 模型数据
         */
        modeldata ,

        /**
         * api模型数据
         */
        api_modeldata ,


        integration_modeldata ,

        cluster_modeldata ,

        metabase_inner;


    }

    /**
     * Gson实例在调用Json操作时不保持任何状态,可以共享
     */
    protected Gson gson = new Gson();

    protected final LinkedBlockingQueue<DefaultMqProcessPipeline> pipelineQueue=new LinkedBlockingQueue<DefaultMqProcessPipeline>();

    public abstract ConsumerType  getConsumerType();

    /**
     * 启动mq处理器流水线
     */
    protected   void startWith(Neo4jConfig neo4jConfig, PgWalConfig pgWalConfig)
    {
        int pipeWorkCount=ConsumerType.metabase.equals(this.getConsumerType())?pgWalConfig.getPipeWorkCount(): 64;
        for (int i = 0; i < pipeWorkCount; i++) {
            DefaultMqProcessPipeline pipeline=createProcessPipeline(i);
            if (pipeline != null) {
                pipeline.setNeo4jConfig(neo4jConfig);
                pipeline.addHandlers();
                Neo4jDataRepositoryContext.init(neo4jConfig,i);
                pipelineQueue.add(pipeline);
            }

        }

    }



    protected DefaultMqProcessPipeline  createProcessPipeline(int  pipeId)
    {
        DefaultMqProcessPipeline pipeline=null;
        switch (this.getConsumerType())
        {
            case metabase:
                pipeline= new MqMetabase2Neo4jProcessPipeline(pipeId);
                break;
            case modeldata:
                pipeline= new MqModelData2Neo4jProcessPipeline(pipeId);
                break;
            case api_modeldata:
                pipeline= new MqModelData2Neo4jProcessPipeline(pipeId);
                break;
            case integration_modeldata:
                pipeline= new MqModelData2Neo4jProcessPipeline(pipeId);
                break;
            case cluster_modeldata:
                pipeline= new MqModelData2Neo4jProcessPipeline(pipeId);
                break;
            default:

        }
        return pipeline;
    }

    /**
     * 消费数据响应ack给rabbitmq
     * @param success 是否消费成功
     * @param data 数据内容
     * @param deliveryTag
     * @param channel
     */
    protected void onMsgAck(Boolean success,String data,  long deliveryTag, Channel channel)
    {

        try{
            if (success) {
                // RabbitMQ的ack机制中，第二个参数返回true，表示需要将这条消息投递给其他的消费者重新消费
                channel.basicAck(deliveryTag, false);
            } else {
                // 第三个参数true，表示这个消息会重新进入队列
                channel.basicNack(deliveryTag, false, true);
            }
        }
        catch (Exception ex)
        {
            log.error(" ack mq data error!data=>>： "+data,ex) ;
//            reconnect( success,   deliveryTag,  channel);
        }

    }

    private void reconnect(Boolean success,  long deliveryTag, Channel channel)
    {
        // 重新建立连接
        Connection connection = channel.getConnection();
        try {
            connection.close();
        } catch (Exception ex3) {
            log.error("Error closing connection", ex3);
        }


        try{
            RabbitConfig rabbitConfig = SpringApplicationContext.getBean(RabbitConfig.class);
            CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
            connectionFactory.setHost(rabbitConfig.getHost());
            connectionFactory.setPort(rabbitConfig.getPort());
            connectionFactory.setUsername(rabbitConfig.getUsername());
            connectionFactory.setPassword(rabbitConfig.getPassword());
            connectionFactory.setVirtualHost(rabbitConfig.getVirtualHost());
            connectionFactory.createConnection();

            if (success) {
                // RabbitMQ的ack机制中，第二个参数返回true，表示需要将这条消息投递给其他的消费者重新消费
                channel.basicAck(deliveryTag, false);
            } else {
                // 第三个参数true，表示这个消息会重新进入队列
                channel.basicNack(deliveryTag, false, true);
            }
        }
        catch (Exception ex2)
        {
            log.error("Error requeueing message,reconnect fail!", ex2);
        }
    }


    protected void onMsgAckMultiple(Boolean success, List<PgWalChangeAck> changeList)
    {
        if(CollectionUtils.isEmpty(changeList))
        {
            return;
        }
        for (PgWalChangeAck  pgWalChangeAck: changeList) {
            onMsgAckMultiple( success,  pgWalChangeAck.getDeliveryTag(), pgWalChangeAck.getChannel());
        }
    }

    protected void onMsgAckMultiple(Boolean success, long deliveryTag, Channel channel)
    {
        /**
         * 纯属为单元测试返回
         */
        if(channel==null)
        {
            return;
        }

        try{
            if (success) {
                // RabbitMQ的ack机制中，第二个参数返回true，表示需要将这条消息投递给其他的消费者重新消费
                channel.basicAck(deliveryTag, false);
            } else {
                // 第三个参数true，表示这个消息会重新进入队列
                channel.basicNack(deliveryTag, false, true);
            }
        }
        catch (Exception ex)
        {
            log.error(" ack mq data error!data=>>： ",ex) ;
//            reconnect( success,    deliveryTag,  channel);

        }

    }

    public LinkedBlockingQueue<DefaultMqProcessPipeline> getPipelineQueue() {
        return pipelineQueue;
    }
}
