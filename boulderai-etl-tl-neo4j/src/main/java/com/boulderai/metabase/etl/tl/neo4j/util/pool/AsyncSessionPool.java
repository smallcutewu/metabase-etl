//package com.boulderai.metabase.sync.core.util.pool;
//
//import com.boulderai.metabase.lang.util.SleepUtil;
//import com.boulderai.metabase.sync.core.config.Neo4jConfig;
//import lombok.extern.slf4j.Slf4j;
//import org.neo4j.driver.*;
//import org.neo4j.driver.async.AsyncSession;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.PostConstruct;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//@Component
//@Slf4j
//public class AsyncSessionPool {
//
//    @Autowired
//    private Neo4jConfig neo4jConfig;
//
//    private  SessionConfig sessionConfig;
//
//    private Config config = Config.builder()
////                .withMaxConnectionLifetime(30, TimeUnit.MINUTES)
////                .withMaxConnectionPoolSize(50)
////                .withConnectionAcquisitionTimeout(2, TimeUnit.MINUTES)
//            .withConnectionLivenessCheckTimeout( 60000, TimeUnit.MILLISECONDS )
//            .build();
//
//    private  LinkedBlockingQueue<AsyncSession> sessionQueue;
//
//    private AtomicInteger  borrowCount=new AtomicInteger(0);
//    private AtomicInteger  returnCount=new AtomicInteger(0);
//
//    @PostConstruct
//    public  void  init( ) {
//        int poolSize=neo4jConfig.getPoolSize();
//        this.sessionQueue = new LinkedBlockingQueue<>(poolSize*3);
//        sessionConfig = SessionConfig.builder().withDefaultAccessMode(AccessMode.WRITE).build();
//        initializePool(poolSize);
//    }
//
//
//
//    private void initializePool(int poolSize) {
//        for (int i = 0; i < poolSize; i++) {
//            AsyncSession session = createSession();
//            sessionQueue.add(session);
//        }
//    }
//
//    private AsyncSession createSession() {
//        Driver driver =GraphDatabase.driver(neo4jConfig.getUri(), AuthTokens.basic(neo4jConfig.getUsername(), neo4jConfig.getPassword()),config);
//        return driver.asyncSession(sessionConfig);
//    }
//
//
//    public AsyncSession borrowSessionSync() {
//        AsyncSession  session=null;
//        int k=0;
//        while(session==null&&k<5)
//        {
//            try {
//                session= sessionQueue.poll(1000, TimeUnit.MILLISECONDS);
//            } catch (Exception e) {
//            }
//            if(session==null)
//            {
//                SleepUtil.sleepMillisecond(250);
//                k++;
//            }
//        }
////        borrowCount.addAndGet(1);
//        if (session == null) {
//            session=this.createSession();
//        }
////        log.info("borrowSession returnCount: "+returnCount.get()+" / borrowCount:"+borrowCount.get()+" / sessionQueue count: "+sessionQueue.size());
//        return session;
//    }
//
//    public AsyncSession borrowSession() {
//        AsyncSession  session=null;
//        try {
//            session= sessionQueue.poll(1000, TimeUnit.MILLISECONDS);
//        } catch (Exception e) {
//        }
//        if(session==null)
//        {
//            session=  createSession();
//        }
//        else
//        {
////            borrowCount.addAndGet(1);
////            log.info("borrowSession returnCount: "+returnCount.get()+" / borrowCount:"+borrowCount.get()+" / sessionQueue count: "+sessionQueue.size());
//
//        }
//        return session;
//    }
//
//    public void returnSession(AsyncSession session) {
//        if (session != null) {
//            sessionQueue.add(session);
////            returnCount.addAndGet(1);
////            log.info("returnSession returnCount: "+returnCount.get()+" / borrowCount:"+borrowCount.get()+" / sessionQueue count: "+sessionQueue.size());
//        }
//    }
//
//    public void close() {
//        for (AsyncSession session : sessionQueue) {
//            session.closeAsync();
//        }
//        sessionQueue.clear();
//    }
//}