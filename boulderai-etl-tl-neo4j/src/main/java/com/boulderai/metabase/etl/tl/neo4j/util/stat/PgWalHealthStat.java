package com.boulderai.metabase.etl.tl.neo4j.util.stat;

import org.joda.time.DateTime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class PgWalHealthStat {

    private static final ConcurrentHashMap<Integer, AtomicInteger> miniConsumeStat=new ConcurrentHashMap<Integer, AtomicInteger>();
    private static final ReentrantLock lock = new ReentrantLock();
    private static long lastActiveTime=0L;

    public  static  void clear()
    {
        lastActiveTime=0L;
        miniConsumeStat.clear();
    }

    public  static  AtomicInteger makeHealth()
    {
        lastActiveTime=System.currentTimeMillis();
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
        if (timeCount != null) {
            timeCount.incrementAndGet();
        }
        return timeCount;

    }
}
