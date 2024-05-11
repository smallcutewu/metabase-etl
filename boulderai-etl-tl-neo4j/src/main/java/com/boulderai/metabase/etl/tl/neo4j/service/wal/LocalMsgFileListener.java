package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.etl.tl.neo4j.util.HandleDataEnv;
import com.boulderai.metabase.etl.tl.neo4j.util.NamedThreadFactory;
import com.boulderai.metabase.etl.tl.neo4j.util.SpringApplicationContext;
import com.boulderai.metabase.etl.tl.neo4j.util.StartFlagger;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;
import com.boulderai.metabase.etl.tl.neo4j.util.fqueue.FQueue;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @ClassName: LocalMsgFileListener
 * @Description: 日志文件处理器
 * @author  df.l
 * @date 2023年06月07日
 * @Copyright boulderaitech.com
 */
@Slf4j
public class LocalMsgFileListener {
    private int threadCount = 10;
    private int maxMsgCount = 100;
    private int tempMinMsgCount = maxMsgCount*2;

    private int maxQueueSize=50000;
    private int minQueueSize=10000;
    private LinkedBlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(maxQueueSize);
    private LinkedBlockingQueue<String> msgDeleteQueue = new LinkedBlockingQueue<>(maxQueueSize);

    private ConcurrentHashMap<DataSynTopics, MqModelDataListenerConsumer> mqConsumerMap = new ConcurrentHashMap<DataSynTopics, MqModelDataListenerConsumer>();
    private ExecutorService executorService;
//    private Production<String> production;
    private  DataSynTopics dataSynTopics;
    private AtomicLong  totalCount=new AtomicLong(0L);
    private final ReentrantLock lock = new ReentrantLock();
    private final ReentrantLock queueLock = new ReentrantLock();
    protected MqModelDataListenerConsumer mqModelDataListenerConsumer = null;
    private final ConcurrentHashMap<Integer, AtomicInteger>  miniConsumeStat=new ConcurrentHashMap<Integer, AtomicInteger>();
    private FQueue localFileQueue =null;

    private int minDeleteQueueSize=5000;
    private  Boolean showDataLog=true;

    public LocalMsgFileListener(FQueue localFileQueue, DataSynTopics dataSynTopics,int threadCount) {
        this.localFileQueue = localFileQueue;
        this.dataSynTopics = dataSynTopics;
        this.threadCount=threadCount;
    }


    public void setDataSynTopics(DataSynTopics dataSynTopics) {
        this.dataSynTopics = dataSynTopics;
    }

    private   AtomicInteger  getMiniConsumeStat()
    {
//        DateTime time =new DateTime();
        int miniNum =DateTime.now().getMinuteOfDay();
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

    public void init() {
        PgWalConfig pgWalConfig= SpringApplicationContext.getBean(PgWalConfig.class);
        this.showDataLog=pgWalConfig.getShowDataLog();
        switch (dataSynTopics)
        {
            case eimos:
                MqEimosModelDataListenerConsumer  mqModelDataListenerConsumer2= SpringApplicationContext.getBean(MqEimosModelDataListenerConsumer.class);
                mqModelDataListenerConsumer=mqModelDataListenerConsumer2;
                break;
            case integration:
                MqIntegrationModelDataListenerConsumer  mqModelDataListenerConsumer3=SpringApplicationContext.getBean(MqIntegrationModelDataListenerConsumer.class);
                mqModelDataListenerConsumer=mqModelDataListenerConsumer3;
                break;
            case integration_cluster:
                MqClusterDataListenerConsumer  mqModelDataListenerConsumer4=SpringApplicationContext.getBean(MqClusterDataListenerConsumer.class);
                mqModelDataListenerConsumer=mqModelDataListenerConsumer4;
                break;
        }
//        executorService = Executors.newFixedThreadPool(threadCount);
//        for (int k = 0; k < threadCount; k++) {
//            executorService.submit(new HandleLocalFileRunnable(k));
//        }

        this.executorService = new ThreadPoolExecutor(
                threadCount, threadCount,
                60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                new NamedThreadFactory("HandleLocalFile-"+dataSynTopics.getQueueName())
        );

        for (int k = 0; k < threadCount; k++) {
            this.executorService.submit(new HandleLocalFileRunnable(k)) ;
        }
    }

    public Boolean  isMsgQueueEmpty()
    {
        return  msgQueue.size()<=minQueueSize;
    }


    public Boolean  isMsgDeleteQueueEmpty()
    {
        return  msgDeleteQueue.size()<=minDeleteQueueSize;
    }


    public int  getQueuSize()
    {
        return  msgQueue.size();
    }

    public int  getDeleteQueuSize()
    {
        return  msgDeleteQueue.size();
    }

    public void addRecordDeleteMsg(String msg) {
        msgDeleteQueue.add(msg);
    }

    public void addNewFileMsg(String msg,boolean isDeleteRec) {
         if(isDeleteRec)
         {
             try {
                 msgDeleteQueue.put(msg);
             } catch (Exception e) {
                 log.error("  msgDeleteQueue.put(msg)  error! ",e);
             }
             return;
         }

        if (msgQueue.size()>=maxQueueSize) {
            log.info(dataSynTopics.getDesc()+" local mem queue has full!");
        }
        try {
            msgQueue.put(msg);
        } catch (Exception e) {
            log.error("  msgQueue.put(msg)  error! ",e);
        }
    }

    private class HandleLocalFileRunnable implements Runnable {
        private int id;
        private long lastDealTime = System.currentTimeMillis();
        protected long max_waiting_time = 1000 * 60;
//        protected TransmittableThreadLocal<Long> lastOperTimeMap = new TransmittableThreadLocal<Long>();
        protected Gson gson = new Gson();
        private HashSet<String>  tableSet=new HashSet<String>();
        private  Boolean  noStopFlag=true;


        public HandleLocalFileRunnable(int id) {
            this.id = id;

        }


        private  void doHandleDetail()
        {
            Long lastOperTime = HandleDataEnv.getLastHandleTime();
            if (lastOperTime == null) {
                lastOperTime = System.currentTimeMillis();
                HandleDataEnv.addLastHandleTime();
            }

            Boolean  timeExpired =System.currentTimeMillis() - lastOperTime > max_waiting_time;
            if (timeExpired|| msgQueue.size() >= tempMinMsgCount ||!msgDeleteQueue.isEmpty()) {
                if (msgQueue.isEmpty()&&msgDeleteQueue.isEmpty()) {
                    log.info("Thread"+id+" "+dataSynTopics.getDesc()+" 时间已过期！本地组内存队列是空的");
                    SleepUtil.sleepSecond(10);
                    return ;
                }
            } else {
                SleepUtil.sleepSecond(2);
                return;
            }



            if (msgQueue.isEmpty()&&msgDeleteQueue.isEmpty()) {
                log.info("Thread"+id+" "+dataSynTopics.getDesc()+"   本地内存队列是空的");
                SleepUtil.sleepSecond(5);
                return;
            }

            if(timeExpired)
            {
                log.info("Thread"+id+" "+dataSynTopics.getDesc()+" time  expired! sys will fork to exec wal records in queue ! size: "+msgQueue.size());
            }

            List<String> msgList = new ArrayList<>();
            queueLock.lock();
            try {
                if(!msgDeleteQueue.isEmpty())
                {
                    msgDeleteQueue.drainTo(msgList, maxMsgCount);
                }
                if(msgList.isEmpty()&&!msgQueue.isEmpty())
                {
                    msgQueue.drainTo(msgList, maxMsgCount);
                }
            }
            catch (Exception  ex2)
            {
                log.error("  msgQueue.drainTo  error!",ex2);
            }
            finally {
                queueLock.unlock();
            }

            if (msgList.isEmpty()) {
                log.info("Thread"+id+" "+dataSynTopics.getDesc()+" 从本地队列竞争获取消息为空");
                SleepUtil.sleepSecond(3);
                return;
            }
//            Collections.shuffle(msgList);
            tableSet.clear();
            List<PgWalChangeAck> changeList = new ArrayList<>();
            for (String data : msgList) {
                PgWalChange record = null;
                try {
                    record = gson.fromJson(data, PgWalChange.class);
                } catch (Exception ex) {
                    log.error("gson fromJson error!", ex);
                }
                if(record != null&&record.getTable()!=null)
                {
                    tableSet.add(record.getTable());
                }
                PgWalChangeAck changeInfo = new PgWalChangeAck(record, 0, null);
                changeList.add(changeInfo);
            }
            if(changeList.isEmpty())
            {
                log.info("Thread"+id+" "+dataSynTopics.getDesc()+" 从主队列竞争得到临时队列是空的");
                return;
            }

            Boolean result =false;
            try
            {
                result =   mqModelDataListenerConsumer.consumerPgLocalWalChange(changeList, 1);
            }
            catch (Exception  ex)
            {
                log.error(" mqModelDataListenerConsumer.consumerPgLocalWalChange  error!",ex);
            }

            totalCount.incrementAndGet();
            if (!result) {
                for (String msg : msgList) {
                    byte[]   bytes=null;
                    try {
                        bytes = msg.getBytes("UTF-8");
                        if(bytes!=null&&bytes.length>0)
                        {
                            localFileQueue.offer(bytes);
                        }
                    } catch( Exception e) {
                        log.error(" localFileQueue.offere  error!",e);
                    }
                }
                log.error("Thread"+id+" "+dataSynTopics.getDesc()+"  do "+totalCount.get()+" time execute batch cql fail!  record count: "+changeList.size()+"  tables: "+ Arrays.toString(tableSet.toArray()));
            }
            else
            {
                log.info("Thread"+id+" "+dataSynTopics.getDesc()+"  do  "+totalCount.get()+" time execute batch cql success! record count:"+changeList.size()+"  tables: "+  Arrays.toString(tableSet.toArray()));
                if(showDataLog)
                {
                    for (String  msg:  msgList) {
                        // todo 精简日志
                        log.info("###Sync data ok! "+msg.substring(0,100));
                    }
                }

            }

            getMiniConsumeStat().addAndGet(changeList.size());
            if(totalCount.get()%50==0)
            {
              log.info("分钟tps统计：/r/n    "+ printConsumeStat());
            }

            if (totalCount.get()>=Integer.MAX_VALUE) {
                totalCount.set(0L);
            }
            msgList.clear();
            changeList.clear();
        }

        @Override
        public void run() {
            HandleDataEnv.addDataSynTopics(dataSynTopics);
            while (noStopFlag) {
                if ( !StartFlagger.isOK()) {
                    log.info("LocalMsgFileListener  Thread"+id+" "+dataSynTopics.getDesc()+"  wal 监听未启动 请稍等.........");
                    SleepUtil.sleepSecond(5);
                    continue;
                }


                if (msgQueue.isEmpty()&&msgDeleteQueue.isEmpty()) {
                    log.info("Thread"+id+" "+dataSynTopics.getDesc()+"   本地组内存队列是空的");
                    SleepUtil.sleepSecond(20);
                    continue;
                }

                try
                {
                    doHandleDetail();
                }
                catch (Exception  ex )
                {
                  log.error("LocalMsgFileListener  Thread "+id+" "+dataSynTopics.getDesc()+   "doHandleDetail   uncatch error!",ex );
                  SleepUtil.sleepSecond(2);
                }


            }

            for(int i=0;i<200;i++)
            {
                log.error("LocalMsgFileListener  Thread"+id+" "+dataSynTopics.getDesc()+"  跳出任务循环了！！！.........");
                SleepUtil.sleepSecond(5);
            }

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
            sb.append(c.getKey() ).append(" : ").append(c.getValue()).append(" \r\n ");
            k++;
            if(k>100)
            {
                break;
            }
        }
        return sb.toString();
    }


}

