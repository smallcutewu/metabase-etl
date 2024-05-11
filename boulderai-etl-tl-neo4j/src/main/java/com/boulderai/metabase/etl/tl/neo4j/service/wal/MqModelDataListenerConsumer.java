package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.alibaba.ttl.TransmittableThreadLocal;
import com.boulderai.metabase.etl.tl.neo4j.service.pipeline.DefaultMqProcessPipeline;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.etl.tl.neo4j.util.StartFlagger;
import com.boulderai.metabase.etl.tl.neo4j.util.Stopper;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ClassName: MqModelDataListenerConsumer，真实模型数据处理
 * @Description: rabbitmq消费客户端监听器
 * @author  df.l
 * @date 2022年10月17日
 * @Copyright boulderaitech.com
 */

@Slf4j
public class MqModelDataListenerConsumer extends   MqBaseListenerConsumer {

    protected TransmittableThreadLocal<Long> lastOperTimeMap = new TransmittableThreadLocal<Long>();

    private AtomicInteger   pipleIdFlag=new AtomicInteger(1000);
    protected Integer  baseCount=100;

    @Autowired
    protected Neo4jConfig neo4jConfig;

    @Autowired
    protected  PgWalConfig pgWalConfig;

    protected  AtomicLong logCount=new AtomicLong(0L);
    protected  static  final   long MAX_LOG_COUNT=Long.MAX_VALUE-1000000;

    protected final LinkedBlockingQueue<PgWalChangeAck> tempChangeList=new LinkedBlockingQueue<PgWalChangeAck>();
    protected  int dealDataCount=200;
    private long lastDealTime=System.currentTimeMillis();
    protected   long max_waiting_time=1000*60;
//    private ReentrantLock lock = new ReentrantLock();
//    private java.util.concurrent.Semaphore semaphore = new Semaphore(5);

    protected  Boolean batchUpdateData=false;

    private Map<String,Integer> schemaMap=new HashMap<>();

//    private  static AtomicInteger  receiceCount =new AtomicInteger(0);

    @Override
    public  ConsumerType  getConsumerType()
    {
        return ConsumerType.modeldata;
    }

    public   void start()
    {
        schemaMap.put(DataSynConstant.METABASE_WAL_SCHEMA,1);
        this.startWith( neo4jConfig,  pgWalConfig);
    }


    public void consumerPgWalChange(String data, long deliveryTag, Channel channel,int dataType)
    {
        //其他组件没有准备好，请稍等
        if(!Stopper.isRunning()|| !StartFlagger.isOK())
        {
            log.info(this+"  consumer receive  but Stopper not ok! " + data);
            onMsgAck(false, data,   deliveryTag,  channel);
            SleepUtil.sleepSecond(10);
            return  ;
        }

         if(StringUtils.isBlank(data))
         {
             log.info(this+"  consumer receive  but data  empty ! " + data);
             onMsgAck(true, data,   deliveryTag,  channel);
            return  ;
         }
         long count=logCount.get();
         if (count%baseCount==0) {
             if(dataType==1)
             {
                 log.info(this+"  consumer receive integration  model data : " + data);
             }
             else {
                 log.info(this +" consumer receive eimos  model data : " + data);
             }

         }
         if (count>=MAX_LOG_COUNT) {
             logCount.set(0L);
         }
        logCount.addAndGet(1L);

         //转换数据
        PgWalChange record =null;
        try {
            record = gson.fromJson(data, PgWalChange.class);
        }
        catch(Exception ex)
        {
            log.error("gson fromJson error!",ex);
        }
        if(record==null)
        {
            onMsgAck(true, data,   deliveryTag,  channel);
            return  ;
        }


        //空转模式快速消费消息
        if(MSG_DEBUG_MANAGER.getEmptyConsumeModelData())
        {
            log.info(this +" consumer receive data but empty model now : " + data);
            onMsgAck(true, data,   deliveryTag,  channel);
            return  ;
        }

        List<PgWalChangeAck>  changeList=null;
        PgWalChangeAck  changeInfo=new PgWalChangeAck(record,  deliveryTag, channel);
        if(batchUpdateData)
        {
            onMsgAck(true, data,   deliveryTag,  channel);
           String schema= record.getSchema();
           if(schemaMap.containsKey(schema))
           {
                //推动消息
               log.info("收到推动消息！！！");
           }
           else
           {
               tempChangeList.add(changeInfo);
           }

            Long lastOperTime= lastOperTimeMap.get();
            if(lastOperTime==null)
            {
                lastOperTime=System.currentTimeMillis();
                lastOperTimeMap.set(lastOperTime);
            }
            if(tempChangeList.size()>=dealDataCount|| (System.currentTimeMillis() - lastOperTime > max_waiting_time))
            {

            }
            else
            {
                return;
            }

            if (tempChangeList.isEmpty()) {
                return;
            }

            changeList=new ArrayList<PgWalChangeAck>();
            tempChangeList.drainTo(changeList,dealDataCount);
            if(changeList.isEmpty())
            {
                return;
            }
        }
        else {
            String schema= record.getSchema();
            if(schemaMap.containsKey(schema))
            {
                onMsgAck(true, data,   deliveryTag,  channel);
                return;
            }
        }


            //从队列中取出流水线开始处理数据
            DefaultMqProcessPipeline pipeline=null;
            if(pipelineQueue.isEmpty())
            {
                int  pipeId=this.pipleIdFlag.incrementAndGet();
                pipeline=this.createProcessPipeline(pipeId);
            }
            else
            {
                try {
                    pipeline= pipelineQueue.poll(20, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    log.error("pipelineQueue  poll error!",ex);
                }
            }

            if(pipeline!=null)
            {
                try {
//                    semaphore.acquire();
                    if(batchUpdateData)
                    {
                        pipeline.addChanges(changeList);
                    }
                    else
                    {
                        pipeline.addChange(changeInfo);
                    }
                    pipeline.handleDataList();

                    if (!batchUpdateData) {
                        onMsgAck(true, data,   deliveryTag,  channel);
                    }

                }
                catch (Exception ex)
                {
                    log.error("handle change data error!",ex);
                    if (!batchUpdateData) {
                        onMsgAck(false, data,   deliveryTag,  channel);
                    }

                }
                finally {
                    lastDealTime=System.currentTimeMillis();
                    lastOperTimeMap.set(System.currentTimeMillis());
//                    semaphore.release();
                    pipeline.clearChange();
                    pipelineQueue.add(pipeline);


                }

            }
            else {
                log.error(this+"  pipeline  queue is empty error!");
                if (!batchUpdateData) {
                    onMsgAck(false, data,   deliveryTag,  channel);
                }
            }


    }


    public Boolean consumerPgLocalWalChange(List<PgWalChangeAck>  changeList, int dataType)
    {
//        int random= (int) (Math.random()*10);
//        if(random<=2)
//        {
//            throw   new NullPointerException("空指针测试!");
//        }
        DefaultMqProcessPipeline pipeline=null;
        if(pipelineQueue.isEmpty())
        {
            int  pipeId=this.pipleIdFlag.incrementAndGet();
            pipeline=this.createProcessPipeline(pipeId);
        }
        else
        {
            try {
                pipeline= pipelineQueue.poll(20, TimeUnit.SECONDS);
            } catch (Exception ex) {
                log.error("pipelineQueue  poll error!",ex);
            }
        }

        try {
                pipeline.addChanges(changeList);
                pipeline.handleDataList();
                return true;
            }
            catch (Exception ex)
            {
                log.error("consumerPgLocalWalChange change data error!",ex);
                return false;
            }
            finally {
                pipeline.clearChange();
                pipelineQueue.add(pipeline);
            }



    }

    public void setNeo4jConfig(Neo4jConfig neo4jConfig) {
        this.neo4jConfig = neo4jConfig;
    }

    public void setPgWalConfig(PgWalConfig pgWalConfig) {
        this.pgWalConfig = pgWalConfig;
    }
}
