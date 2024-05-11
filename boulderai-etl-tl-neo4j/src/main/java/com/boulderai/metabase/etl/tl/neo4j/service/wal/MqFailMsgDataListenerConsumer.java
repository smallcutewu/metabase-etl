package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepository;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.etl.tl.neo4j.util.StartFlagger;
import com.boulderai.metabase.etl.tl.neo4j.util.Stopper;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

@Component
@Order(Integer.MAX_VALUE-2000)
@Slf4j
public class MqFailMsgDataListenerConsumer extends MqModelDataListenerConsumer{

    @Autowired
    private Neo4jDataRepository neo4jDataRepository;

//
//    @Autowired
//    private Neo4jDataRepository neo4jDataRepository;

    private ReentrantLock  tempLock=new ReentrantLock();


    protected  ThreadLocal<Long> lastOperTimeMap = new ThreadLocal<Long>();

    protected final LinkedBlockingQueue<String> tempChangeList=new LinkedBlockingQueue<String>();

    public MqFailMsgDataListenerConsumer()
    {
        this.baseCount=20;
        this.batchUpdateData=false;
    }


    /**
     * 消费模型元数据队列信息建立模型节点记忆关系
     * @param    data
     * @param   deliveryTag
     * @param   channel
     * autoDelete = "true"
     */
    @RabbitListener(bindings = @QueueBinding(value = @Queue(value = DataSynConstant.MQ_FAILMSG_DATA_QUEUE, durable = "true"),
            exchange = @Exchange(value = DataSynConstant.MQ_EXCHANGE_FAILMSG_DATA_VALUE, type = ExchangeTypes.FANOUT), key = DataSynConstant.MQ_ROUTING_FAILMSGI_DATA_KEY), ackMode = "MANUAL"
            ,concurrency =  "6"
    )
    public void consumerPgWalChange(String data, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag, Channel channel)
    {
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
        long count=logCount.get();
        if (count%baseCount==0) {
            log.info("consumer receive FailMsg  model data : " + data);
        }
        logCount.incrementAndGet();
        tempChangeList.add(data);

        Long lastOperTime= lastOperTimeMap.get();
        if(lastOperTime==null)
        {
            lastOperTime=System.currentTimeMillis();
            lastOperTimeMap.set(lastOperTime);
        }
        onMsgAck(true, data,   deliveryTag,  channel);
        if(tempChangeList.size()>=baseCount
                || (System.currentTimeMillis() - lastOperTime > max_waiting_time))
        {
            tempLock.lock();
            try {
//            neo4jDataRepository.operNeo4jDataByCqlSingle(data);
                List<String > cqlList=new ArrayList<String>();
                tempChangeList.drainTo(cqlList,baseCount);
                if (cqlList.isEmpty()) {
                    return;
                }
                Boolean  result=   neo4jDataRepository.executeSmallBatchCql(cqlList);
                if (!result) {
                    neo4jDataRepository.executeOneCql(cqlList);
                }
            }
            catch (Exception ex)
            {
                log.error("handle FailMsg data error!",ex);
            }
            finally {
                tempLock.unlock();
                lastOperTimeMap.set(System.currentTimeMillis());
            }

        }


    }



}
