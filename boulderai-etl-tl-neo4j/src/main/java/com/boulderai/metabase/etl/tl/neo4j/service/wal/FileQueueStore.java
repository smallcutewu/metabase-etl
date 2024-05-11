package com.boulderai.metabase.etl.tl.neo4j.service.wal;


import com.boulderai.common.queue.FileQueue;
import com.boulderai.common.queue.Production;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import lombok.extern.slf4j.Slf4j;
import com.boulderai.metabase.etl.tl.neo4j.util.fqueue.FQueue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ClassName: FileQueueStore
 * @Description: 文件队列存贮器
 * @author  df.l
 * @date 2022年12月03日
 * @Copyright boulderaitech.com
 */
@Slf4j
public class FileQueueStore {
    private FileQueue<String> fileQueue;
    private MqMemStore   mqMemStore;

    public ClusterMqStore clusterMqStore;

    private final AtomicLong dataCount=new AtomicLong(0L);
    private Production<String> production;
    private  FQueue localFileQueue =null;

    public FileQueueStore(String queueName,RabbitTemplate rabbitTemplate,Integer  threadCount) {
        String path= DataSynConstant.QUEUE+ File.separator + queueName ;
        int maxGNum=4;
        if(DataSynConstant.WAL_INTEGRATION_MODEL_DATA_QUEUE_NAME.equals(queueName))
        {
            maxGNum=6;
        }
        try {
            localFileQueue = new FQueue(path, maxGNum * 1024 * 1024);
//            fileQueue = FileQueue.ordinary(String.class, queueName);
//            production=fileQueue.getProduction();
        } catch (Exception e) {
            log.error(queueName+" fileQueueStore  start error!",e);
        }
        if(localFileQueue!=null)
        {
            mqMemStore=new MqMemStore(queueName,fileQueue,rabbitTemplate,dataCount,threadCount,production,localFileQueue);
            // 集成数据再初始化一个集群消息store
            if(DataSynConstant.WAL_INTEGRATION_MODEL_DATA_QUEUE_NAME.equals(queueName))
            {
                clusterMqStore = new ClusterMqStore(DataSynConstant.MQ_INTEGRATION_CLUSTER_QUEUE,rabbitTemplate,dataCount,threadCount,localFileQueue);
            }
//            mqMemStore.setThreadCount(threadCount);
        }

    }


    public void close() {
        if (this.mqMemStore!=null) {
            this.mqMemStore.close();
        }

    }

    public void clearStat()
    {
        if (mqMemStore!=null) {
            mqMemStore.clearStat();
        }

    }

    public  String printConsumeStat()
    {
        return mqMemStore.printConsumeStat();
    }

    public void sendProduction(String data,boolean isDeleteRec)
    {
//        if(isDeleteRec&&mqMemStore.canDeleteFirst())
//        {
//            mqMemStore.addDeleteMsg(data);
//            return;
//        }
        byte[]  bytes=null;
        try {
            bytes = data.getBytes("UTF-8");
            if (bytes != null) {
                boolean saveRes =false;
                for(int i=0;i<5;i++)
                {
                    saveRes = localFileQueue.offer(bytes);
                    if(saveRes)
                    {
                        dataCount.incrementAndGet();
                        break;
                    }
                }
            }
        } catch (Exception e) {
           log.error("sendProduction  error!",e);
        }


//        production.put(data);

    }


    public AtomicLong getDataCount() {
        return dataCount;
    }
}
