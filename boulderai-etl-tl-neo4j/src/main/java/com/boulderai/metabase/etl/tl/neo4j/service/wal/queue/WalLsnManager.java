package com.boulderai.metabase.etl.tl.neo4j.service.wal.queue;

import com.boulderai.metabase.etl.tl.neo4j.util.DbOperateUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class WalLsnManager {
    private final static String  BATCH_UPDATE_SQL=" insert into public.pg_wal_last_receive_lsn (id,slot_name,last_receive_lsn,update_time)" +
            " values (?,?,?,?) ON CONFLICT  (id) do update set slot_name =?,last_receive_lsn=?,update_time=? ";
    private String slotName;
    private List<WalLsnRecord> lsnList=new ArrayList<WalLsnRecord>();
    private final  static Integer QUEUE_SIZE=30;
    private ScheduledExecutorService executorService;
    private final LinkedBlockingQueue<WalLsnRecord>   walLsnRecordQueue=new LinkedBlockingQueue<WalLsnRecord>(QUEUE_SIZE);
    private WalLsnManager()
    {

    }

    public static WalLsnManager getInstance() {
        return  Holder.INSTANCE;
    }

    public  void init()
    {
        for(int i=1;i<=QUEUE_SIZE;i++)
        {
            if(walLsnRecordQueue.size()<QUEUE_SIZE)
            {
                WalLsnRecord  record=new WalLsnRecord(i,slotName);
                walLsnRecordQueue.add(record);
            }

        }

//        executorService = Executors.newSingleThreadScheduledExecutor(r -> {
//            Thread t = new Thread(r, " WalLsnManager Check");
//            // 设置线程为守护线程，主线程退出，子线程也随之退出
//            t.setDaemon(true);
//            return t;
//        });
//        executorService.scheduleWithFixedDelay(new SavePgLastLsnRunnable(),1,1, TimeUnit.MINUTES);

    }

    public Boolean  saveLastLsn()
    {
        lsnList.clear();
        for (WalLsnRecord  walLsnRecord :walLsnRecordQueue ) {
            if(walLsnRecord.getChange())
            {
                lsnList.add(walLsnRecord);
            }
        }
        if(!lsnList.isEmpty())
        {
            int k=0;
            Object[][] params = new Object[lsnList.size()][];
            for (WalLsnRecord walLsnRecord : lsnList) {
                params[k] = new Object[]{
                        walLsnRecord.getId(),
                        walLsnRecord.getSlotName(),
                        walLsnRecord.getLastReceiveLsn(),
                        walLsnRecord.getUpdateTime(),
                        walLsnRecord.getSlotName(),
                        walLsnRecord.getLastReceiveLsn(),
                        walLsnRecord.getUpdateTime()
                };

                k++;

            }

            DbOperateUtil.batchInsert(BATCH_UPDATE_SQL,params);
            params=null;
            return   true;
        }
        return   false;
    }

    private class  SavePgLastLsnRunnable implements Runnable {

        @Override
        public void run() {
            saveLastLsn();
        }
    }

    public void setLastLsn(String   lastReceiveLsn)
    {
        WalLsnRecord  record=walLsnRecordQueue.poll();
        if(record!=null)
        {
            record.setLastReceiveLsn(lastReceiveLsn);
            walLsnRecordQueue.add(record);
        }
    }

    public WalLsnManager setSlotName(String slotName) {
        this.slotName = slotName;
        return this;
    }

    private static  class Holder
    {
        static final  WalLsnManager INSTANCE=new WalLsnManager();
    }
}
