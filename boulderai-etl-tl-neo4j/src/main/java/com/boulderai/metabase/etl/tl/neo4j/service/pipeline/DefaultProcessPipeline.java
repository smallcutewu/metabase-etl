//package com.boulderai.metabase.sync.core.service.pipeline;
//
//import com.boulderai.metabase.sync.core.config.Neo4jConfig;
//import com.boulderai.metabase.sync.core.service.DefaultPgDataSyncService;
//import com.boulderai.metabase.sync.core.service.handler.DefaultDataHandler;
//import com.boulderai.metabase.sync.core.service.handler.FilterDataHandler;
//import com.boulderai.metabase.sync.core.service.handler.IDataHandler;
//import com.boulderai.metabase.sync.core.service.neo4j.Neo4jDataRepository;
//import com.boulderai.metabase.sync.core.service.neo4j.Neo4jDataRepositoryContext;
//import com.boulderai.metabase.sync.core.service.wal.model.PgWalChange;
//import com.boulderai.metabase.sync.core.util.Stopper;
//import org.apache.commons.collections4.CollectionUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.util.StopWatch;
//
//import java.util.*;
//import    java.util.concurrent.ConcurrentLinkedDeque;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.TimeUnit;
//
///**
// * @ClassName: DefaultProcessPipeline
// * @Description: 默认的Pipeline实现，责任链模式
// * @author  df.l
// * @date 2022年10月11日
// * @Copyright boulderaitech.com
// */
//public  class DefaultProcessPipeline implements  IProcessPipeline {
//    private  final static Logger logger = LoggerFactory.getLogger(DefaultProcessPipeline.class);
//
//    /**
//     * 存贮默认数据处理器，先进先出处理
//     */
//    private final ConcurrentLinkedDeque<IDataHandler> dataHandlerQueue = new ConcurrentLinkedDeque<IDataHandler>();
//
//    private static  final int QUEUE_BUFFER_MAX_SIZE=60000;
//
//    /**
//     * 存放wal消息队列
//     */
//    private final LinkedBlockingQueue<PgWalChange> walRecordChangeQueue = new LinkedBlockingQueue<PgWalChange>(QUEUE_BUFFER_MAX_SIZE);
//
//    private  int pipId;
//    private Neo4jConfig neo4jConfig;
//
//    public void setNeo4jConfig(Neo4jConfig neo4jConfig) {
//        this.neo4jConfig = neo4jConfig;
//    }
//
//    public DefaultProcessPipeline(int pipId)
//    {
//        this.pipId=pipId;
//    }
//
//    @Override
//    public  void addHandlers()
//    {
//        this.addFirst(new DefaultDataHandler());
//        this.addLast(new FilterDataHandler());
//    }
//
//    @Override
//    public Boolean isQueueFull() {
//        return walRecordChangeQueue.size() >= QUEUE_BUFFER_MAX_SIZE;
//    }
//
//
//    /**
//     * 初始化数据处理器
//     */
//    @SuppressWarnings("AlibabaAvoidManuallyCreateThread")
//    @Override
//    public void start() {
//        Thread thread=new Thread(new HandleWalDataRunnable());
//        thread.setName("Queue_HandleWalData_Thread");
//        thread.setDaemon(true);
//        thread.start();
//    }
//
//    /**
//     * 数据来了，开始通过处理器流水线
//     *
//     * @param dataList wal数据
//     */
//    @Override
//    public void handleWalData(final List<PgWalChange> dataList) {
////        StopWatch sw = new StopWatch("test11");
//        dataHandlerQueue.stream().forEach(handler ->
//        {
////           sw.start(handler.getClass().getName());
//
//            if (CollectionUtils.isEmpty(dataList)) {
//                return;
//            }
//            handler.process(dataList);
////            sw.stop();
////            System.out.println(sw.prettyPrint());
//        });
//
//    }
//
//    @Override
//    public Boolean addWalRecord(PgWalChange record) {
//        int count=0;
//        while(walRecordChangeQueue.size()>=QUEUE_BUFFER_MAX_SIZE)
//        {
//            try {
//                TimeUnit.MILLISECONDS.sleep(300);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            count++;
//
//            if(count%20==0)
//            {
//                logger.warn("Wal record change queue ["+this.pipId+"] is full. Waiting now ......! queue size: "+walRecordChangeQueue.size());
//
//            }
//
//            if(count>=10)
//            {
//                return false;
//            }
//
//        }
//        walRecordChangeQueue.add(record);
//        return true;
//
//    }
//
//    /**
//     * 将handler加入队列最前面
//     *
//     * @param handleName
//     * @param dataHandler
//     * @return 返回pipeline本身，链式编程
//     */
//    @Override
//    public IProcessPipeline addFirst(String handleName, IDataHandler dataHandler) {
//        dataHandlerQueue.addFirst(dataHandler);
//        return this;
//    }
//
//    public IProcessPipeline addFirst(IDataHandler dataHandler) {
//        dataHandlerQueue.addFirst(dataHandler);
//        return this;
//    }
//
//
//    /**
//     * 将handler加入队列最后面
//     *
//     * @param handleName
//     * @param dataHandler
//     * @return 返回pipeline本身，链式编程
//     */
//    @Override
//    public IProcessPipeline addLast(String handleName, IDataHandler dataHandler) {
//        dataHandlerQueue.addLast(dataHandler);
//        return this;
//    }
//
//    public IProcessPipeline addLast(IDataHandler dataHandler) {
//        dataHandlerQueue.addLast(dataHandler);
//        return this;
//    }
//
//    private class HandleWalDataRunnable implements Runnable {
//
//        public HandleWalDataRunnable() {
//
//        }
//
//        @Override
//        public void run() {
//            Neo4jDataRepositoryContext.init(neo4jConfig);
//            List<PgWalChange>  recordList=new LinkedList<PgWalChange>();
//            Random  random=new Random();
//
//            while(Stopper.isRunning())
//            {
//                if(walRecordChangeQueue.isEmpty())
//                {
//                    try {
//                        TimeUnit.MILLISECONDS.sleep(500+random.nextInt(500));
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    continue;
//                }
//
//                if(!recordList.isEmpty())
//                {
//                    recordList.clear();
//                }
//
//                //批量处理
//                walRecordChangeQueue.drainTo(recordList,100);
//                if(!recordList.isEmpty())
//                {
//                    StopWatch sw = new StopWatch("test");
//                    sw.start("task1");
//                    DefaultProcessPipeline.this.handleWalData(recordList);
//
////                    System.out.println(sw.prettyPrint());
//                }
//                else
//                {
//                    try {
//                        TimeUnit.MILLISECONDS.sleep(500+random.nextInt(500));
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//
//            //发出停止命令将队列的消息最后一次全部poll出去出去
//            if(!walRecordChangeQueue.isEmpty())
//            {
//                recordList.clear();
//                walRecordChangeQueue.drainTo(recordList,100);
//                if(!recordList.isEmpty())
//                {
//                    DefaultProcessPipeline.this.handleWalData(recordList);
//                }
//            }
//            Neo4jDataRepositoryContext.remove();
//        }
//    }
//
//
//}
