package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.common.queue.FileQueue;
import com.boulderai.common.queue.Production;
import com.boulderai.metabase.context.env.DefaultEnvironment;
import com.boulderai.metabase.context.env.EnvironmentDefinite;
import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.util.*;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;
import com.boulderai.metabase.etl.tl.neo4j.util.fqueue.FQueue;
import org.joda.time.DateTime;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @ClassName: MqMemStore
 * @Description: 本地数据到Mq转换存贮器
 * @author  df.l
 * @date 2022年12月02日
 * @Copyright boulderaitech.com
 */
@Slf4j
public class MqMemStore  implements IStore{
    private static final int CPU_NUMBERS = Runtime.getRuntime().availableProcessors();
//    private static final int THD_NUMBERS = CPU_NUMBERS << 1;
private static final int THD_NUMBERS = 2;
    private static final int PER_CONTEXTS = 10240;
    private static final int MAX_CONTEXTS = PER_CONTEXTS * 2;
    private  ThreadPoolExecutor executor;
    private final String  groupId;
    private final FileQueue<String> fileQueue;
    private final RabbitTemplate rabbitTemplate;
    private  DataSynTopics dataSynTopics;
    private final AtomicLong leftCount;
    private final ConcurrentHashMap<Integer, AtomicInteger>  miniConsumeStat=new ConcurrentHashMap<Integer, AtomicInteger>();
    private final ReentrantLock  lock = new ReentrantLock();
    private Integer  threadCount=2;
//    private  MqModelDataListenerConsumer   mqModelDataListenerConsumer;

    private  LocalMsgFileListener   localMsgFileListener;

    private static final String DELETE_OPER="delete";

    private Production<String> production;

    private FQueue localFileQueue =null;

    private PgWalConfig pgWalConfig = SpringApplicationContext.getBean(PgWalConfig.class);


    public MqMemStore(String  groupId, FileQueue<String> fileQueue
            ,RabbitTemplate rabbitTemplate,AtomicLong leftCount,Integer  threadCount, Production<String> production,
                      FQueue localFileQueue) {
        this.groupId=groupId;
        this.rabbitTemplate=rabbitTemplate;
        this.fileQueue=fileQueue;
        this.leftCount=leftCount;
        int queueCount=MAX_CONTEXTS;
        this.threadCount=threadCount;
        this.production=production;
        this.localFileQueue=localFileQueue;
        Neo4jConfig  neo4jConfig= SpringApplicationContext.getBean(Neo4jConfig.class);
        if(DataSynConstant.WAL_META_QUEUE_NAME.equals(groupId))
        {
            dataSynTopics=DataSynTopics.metabase;
        }
        else   if(DataSynConstant.WAL_NPI_MODEL_DATA_QUEUE_NAME.equals(groupId))
        {
            dataSynTopics=DataSynTopics.npi;
        }
        else   if(DataSynConstant.WAL_EIMOS_MODEL_DATA_QUEUE_NAME.equals(groupId))
        {
            dataSynTopics=DataSynTopics.eimos;
            localMsgFileListener=new LocalMsgFileListener( localFileQueue,  dataSynTopics,neo4jConfig.getEimosThreadCount());
            localMsgFileListener.init();
        }
        else   if(DataSynConstant.WAL_INTEGRATION_MODEL_DATA_QUEUE_NAME.equals(groupId))
        {
            dataSynTopics=DataSynTopics.integration;
            if(pgWalConfig.getClusterSwitch()) {
                localMsgFileListener=new LocalMsgFileListener( localFileQueue,  dataSynTopics,1);
            } else {
                localMsgFileListener=new LocalMsgFileListener( localFileQueue,  dataSynTopics,neo4jConfig.getIntegraThreadCount());
            }

            localMsgFileListener.init();
        }
        else   if(DataSynConstant.WAL_OTHER_MODEL_DATA_QUEUE_NAME.equals(groupId))
        {
            dataSynTopics=DataSynTopics.others;
        }
        else
        {
            dataSynTopics=DataSynTopics.performance_test;
            queueCount=queueCount*10;
            log.error("  file  groupId  not found !"+groupId);
        }

        EnvironmentDefinite env= DefaultEnvironment.getCurrentEnvironment();
        if (env!=null&&env.equals(EnvironmentDefinite.unit_test)) {
            return;
        }
        this.executor = new ThreadPoolExecutor(
                threadCount, threadCount,
                60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCount),
                new NamedThreadFactory("MqMemStore-"+groupId)
        );

        for (int i = 0; i < THD_NUMBERS; i++) {
            this.executor.submit(new ConsumeTask());
        }
    }

    public Boolean  canDeleteFirst()
    {
        return   localMsgFileListener!=null;
    }

    public void  addDeleteMsg(String msg)
    {
        if(localMsgFileListener!=null)
        {
            localMsgFileListener.addRecordDeleteMsg(msg);
        }

    }


    public  String printConsumeStat()
    {
        StringBuilder  sb=new StringBuilder("");
        List<Map.Entry<Integer, AtomicInteger>> list = new ArrayList<Map.Entry<Integer, AtomicInteger>>(miniConsumeStat.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Integer, AtomicInteger>>()
        {
            @Override
            public int compare(Map.Entry<Integer, AtomicInteger> o1, Map.Entry<Integer, AtomicInteger> o2) {
                //按照value值升序
//                return o1.getValue() - o2.getValue();
                //按照value值降序
                return o2.getValue().intValue() - o1.getValue().intValue();
            }
        });

        int k=0;
        for (Map.Entry<Integer, AtomicInteger> c:list) {
            sb.append(c.getKey() ).append(" : ").append(c.getValue()).append(" <br/>");
            k++;
            if(k>100)
            {
                break;
            }
        }
        return sb.toString();
    }

    public void clearStat()
    {
        miniConsumeStat.clear();
    }

    public Integer getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(Integer threadCount) {
        this.threadCount = threadCount;
    }

    private   AtomicInteger  getMiniConsumeStat()
    {
        DateTime time =new DateTime();
        int miniNum =time.getMinuteOfDay();
        AtomicInteger timeCount=   miniConsumeStat.get(miniNum);
        if (timeCount == null) {
            lock.lock();
            try{
                timeCount=   miniConsumeStat.get(miniNum);
                if(timeCount==null)
                {
                    timeCount=new AtomicInteger(0);
                    miniConsumeStat.putIfAbsent(miniNum,timeCount);
                }
            }
            finally {
                lock.unlock();
            }
        }
        return timeCount;

    }

    @Override
    public void close() {
        if ( this.executor!=null) {
            this.executor.shutdownNow();
        }

    }


    private class ConsumeTask implements Runnable {
        @Override
        public void run() {
//            Consumption<String> consumption = fileQueue.getConsumption();
             Gson gson = new Gson();
            while (
//                    consumption != null &&
                            Stopper.isRunning()) {
                try {
//                    String  change= consumption.poll();

                    if(localFileQueue!=null&&localFileQueue.isEmpty())
                    {
                        log.info(dataSynTopics.getDesc()+" local file queue  is  empty");
                        SleepUtil.sleepSecond(30);
                        continue;
                    }

                    if(localMsgFileListener!=null)
                    {
                        if(!localMsgFileListener.isMsgDeleteQueueEmpty())
                        {
                            log.info(dataSynTopics.getDesc()+"msgDeleteQueue size big than  1000 ! waiting empty! size: "+localMsgFileListener.getDeleteQueuSize());
                            SleepUtil.sleepMillisecond(5000);
                            continue;
                        }
                        else  if(!localMsgFileListener.isMsgQueueEmpty())
                        {
                            log.info(dataSynTopics.getDesc()+" local mem queue size big than  5000 ! waiting empty! size: "+localMsgFileListener.getQueuSize());
                            SleepUtil.sleepSecond(5);
                            continue;
                        }

                    }

                    byte[] bytes = localFileQueue.poll();
                    if (bytes == null||bytes.length==0) {
                        SleepUtil.sleepSecond(5);
                        continue;
                    }
                    String change =null;
                    try{
                        change =new String(bytes,"UTF-8");
                    }
                    catch (Exception ex)
                    {
                        log.error("反序列化本地消息粗错误",ex);
                    }

                    if (change != null) {
                        if(dataSynTopics.equals(DataSynTopics.integration)
                                       && !IntegrationDataStopper.isStart())
                        {
                            continue;
                        }

                            try
                            {
                                if(localMsgFileListener!=null)
                                {
                                    PgWalChange record = null;
                                    try {
                                        record = gson.fromJson(change, PgWalChange.class);
                                    } catch (Exception ex) {
                                        log.error("gson fromJson error!", ex);
                                    }
                                    if (record != null) {
                                        boolean isDeleteRec=false;
                                        if (DELETE_OPER .equals(record.getKind())) {
                                            isDeleteRec=true;
                                        }
                                        localMsgFileListener.addNewFileMsg(change,isDeleteRec);
                                    }

                                }
                                else
                                {
                                    log.info("同步数据消息发送rabbitMq msg exchange:{} routeKey:{}",dataSynTopics.getExchangeName(), dataSynTopics.getQueueName());
                                    rabbitTemplate.convertAndSend( dataSynTopics.getExchangeName(),  dataSynTopics.getQueueName(), change);
                                }

                                long  leftNum=leftCount.decrementAndGet();

//                                PgWalChange  pgWalChange = gson.fromJson(change, PgWalChange.class);
//                                String tableName=pgWalChange.getTable();
//                                if (tableName.equals("isc_work_order_line")) {
//                                    System.out.println(change);
//                                }

                                //如果npi或其他eimos数据往others队列发一份给其他应用系统使用
//                                switch (dataSynTopics) {
//                                    case npi:
//                                    case eimos:
//                                        rabbitTemplate.convertAndSend( DataSynTopics.others.getExchangeName(),  DataSynTopics.others.getQueueName(), change);
//                                        break;
//                                }
                                if(DataSynConstant.WAL_META_QUEUE_NAME.equals(groupId))
                                {
                                    log.info(dataSynTopics.getDesc()+"  "+leftNum+" left meta data  to send ! "+change);
                                }
                                else
                                {
                                    if (leftNum%100==0) {
                                        log.info(dataSynTopics.getDesc()+"  "+leftNum+" left model data to send !");
                                    }
                                }
                                if (leftNum==0) {
                                    log.info(groupId+ " in local disk queue left count is zero!");
                                }
                            }
                            catch (Exception e)
                            {
                                log.error(dataSynTopics.getExchangeName()+" rabbitTemplate.convertAndSend  error!",e);
                                SleepUtil.sleepSecond(2);
                            }

                    }
                    else
                    {
                        SleepUtil.sleepSecond(2);
                    }
                }
                catch (Exception ex)
                {
                    log.error(groupId+" fileQueue ConsumeTask  consume error!",ex);
                }

            }

        }

    }
}