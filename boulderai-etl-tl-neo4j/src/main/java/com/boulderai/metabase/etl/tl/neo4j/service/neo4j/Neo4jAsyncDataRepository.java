//package com.boulderai.metabase.sync.core.service.neo4j;
//
//import com.boulderai.metabase.lang.util.SleepUtil;
//import com.boulderai.metabase.sync.core.config.Neo4jConfig;
//import com.boulderai.metabase.sync.core.service.wal.FailMsgManager;
//import com.boulderai.metabase.sync.core.util.pool.AsyncSessionPool;
//import com.boulderai.metabase.sync.core.util.pool.asyc.AsyncSessionFactory;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.pool2.ObjectPool;
//import org.apache.commons.pool2.impl.GenericObjectPool;
//import org.neo4j.driver.*;
//import org.neo4j.driver.async.AsyncSession;
//import org.neo4j.driver.async.ResultCursor;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.PostConstruct;
//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.CompletionStage;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//@Slf4j
//@Component
//public class Neo4jAsyncDataRepository {
//
//    private static final int MAX_POOL_SIZE = 10;
//
//    @Autowired
//    private Neo4jConfig neo4jConfig;
//
//    private  ObjectPool<AsyncSession> sessionPool;
//
//    private int driverCount=50;
//    private LinkedBlockingQueue<Driver>  driverQueue=new LinkedBlockingQueue<Driver>();
//
//    private  Driver driver;
//    private  Driver asyncDriver;
//    private  SessionConfig sessionConfig;
//    private Config config = Config.builder()
////                .withMaxConnectionLifetime(30, TimeUnit.MINUTES)
////                .withMaxConnectionPoolSize(50)
////                .withConnectionAcquisitionTimeout(2, TimeUnit.MINUTES)
//            .withConnectionLivenessCheckTimeout( 60000, TimeUnit.MILLISECONDS )
//            .build();
//
////    private static  AtomicInteger  count =new AtomicInteger(0);
//
//    @Autowired
//    private AsyncSessionPool asyncSessionPool;
//
//    @Autowired
//    private FailMsgManager failMsgManager;
//
//    public Neo4jAsyncDataRepository() {
//
//    }
//
//    @PostConstruct
//    public void init()
//    {
////        AsyncSessionFactory sessionFactory = new AsyncSessionFactory(neo4jConfig.getUri(), neo4jConfig.getUsername(), neo4jConfig.getPassword());
////        GenericObjectPool<AsyncSession> pool = new GenericObjectPool<>(sessionFactory);
////        pool.setMaxTotal(MAX_POOL_SIZE);
////        sessionPool = pool;
//
//
//        sessionConfig = SessionConfig.builder().withDefaultAccessMode(AccessMode.WRITE).build();
//        this.driver = GraphDatabase.driver(neo4jConfig.getUri(), AuthTokens.basic(neo4jConfig.getUsername(), neo4jConfig.getPassword()),config);
//        this.asyncDriver = GraphDatabase.driver(neo4jConfig.getUri(), AuthTokens.basic(neo4jConfig.getUsername(), neo4jConfig.getPassword()),config);
//
//    }
//
//
//
//    private Driver  getUncacheDriver()
//    {
//        Driver driver= null;
//            for (int i = 0; i < 20; i++) {
//                try {
//                    driver= GraphDatabase.driver(neo4jConfig.getUri(), AuthTokens.basic(neo4jConfig.getUsername(), neo4jConfig.getPassword()),config);
//                }
//                catch (Exception ex)
//                {
//                    ex.printStackTrace();
//                    SleepUtil.sleepMillisecond((long) (500+Math.random()*1000));
//                }
//                if (driver != null) {
//                    return driver;
//                }
//            }
//
//
//        return driver;
//    }
//
//
//    private Driver  getCacheDriver()
//    {
//        Driver driver= null;
//        try {
//            driver = driverQueue.poll(500, TimeUnit.MILLISECONDS);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        if (driver == null) {
//            for (int i = 0; i < 20; i++) {
//                try {
//                    driver= GraphDatabase.driver(neo4jConfig.getUri(), AuthTokens.basic(neo4jConfig.getUsername(), neo4jConfig.getPassword()),config);
//                }
//                catch (Exception ex)
//                {
//                    ex.printStackTrace();
//                    SleepUtil.sleepMillisecond((long) (500+Math.random()*1000));
//                }
//                if (driver != null) {
//                    return driver;
//                }
//            }
//
//        }
//        return driver;
//    }
//
//    private void  returnCacheDriver(Driver driver)
//    {
//            if (driver != null) {
//                driverQueue.add(driver);
//            }
//    }
//
//
//
//    public Driver   getDriver()
//    {
//
//        Driver  myDriver = GraphDatabase.driver(neo4jConfig.getUri(), AuthTokens.basic(neo4jConfig.getUsername(), neo4jConfig.getPassword()),config);
//       return myDriver;
//    }
//
//    public SessionConfig  getSessionConfig()
//    {
//        return  sessionConfig;
//    }
//
//    public AsyncSession   getAsyncSession()
//    {
//        return   driver.asyncSession(sessionConfig);
//    }
//
//
//
//
//    public void executeQueryAsync(String query) throws Exception {
//        AsyncSession session = sessionPool.borrowObject();
//        try {
//            CompletionStage<ResultCursor> resultCursorStage = session.runAsync(query);
//            ResultCursor resultCursor = resultCursorStage.toCompletableFuture().get();
//            // 处理结果
//            // ...
//        } catch (Exception ex)
//        {
//            log.error("executeQueryAsync  error!",ex);
//        }
//        finally {
//            sessionPool.returnObject(session);
//        }
//    }
//
//
//
//
//    // 处理结果集
//    private CompletionStage<Void> processResultCursor(ResultCursor cursor) {
//        return cursor.nextAsync().thenComposeAsync(record -> {
//            if (record != null) {
//                // 处理记录
////                Value value = record.get("propertyName");
//                // 打印属性值示例
////                System.out.println(value.asString());
//
//                log.info("one record done ok!");
//
//                // 递归调用以处理下一个记录
//                return processResultCursor(cursor);
//            } else {
//                // 所有记录都已处理完毕
//                return CompletableFuture.completedFuture(null);
//            }
//        });
//    }
//
//    public void executeQueryAsyncList(List<String> queries)  {
//        List<CompletionStage<ResultCursor>> queryResults = new ArrayList<>();
//        AsyncSession session = null;
//        try {
//            session = sessionPool.borrowObject();
//            for (String query : queries) {
//                CompletionStage<ResultCursor> result = session.runAsync(query);
//                queryResults.add(result);
//            }
//
//            for (CompletionStage<ResultCursor> result : queryResults) {
//                result.thenComposeAsync(cursor -> processResultCursor(cursor));
//            }
//
//        }
//        catch (Exception ex)
//        {
//            log.error("executeQueryAsyncList  error!",ex);
//        }
//        finally {
//            try {
//                sessionPool.returnObject(session);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//
//
//    public CompletionStage<Void> runAsyncList(AsyncSession session,List<String> cqlList) {
//        List<CompletionStage<ResultCursor>> queryResults = new ArrayList<>();
//        for (String cql : cqlList) {
//            CompletionStage<ResultCursor> result = session.runAsync(cql);
//            queryResults.add(result);
//        }
//
//        List<CompletionStage<Void>> processingResults = new ArrayList<>();
//        for (CompletionStage<ResultCursor> result : queryResults) {
//            CompletionStage<Void> processingResult = result.thenComposeAsync(cursor -> processResultCursor(cursor));
//            processingResults.add(processingResult);
//        }
//
//        return CompletableFuture.allOf(processingResults.toArray(new CompletableFuture[0]));
//    }
//
//    public void runAsyncList2New(List<String> cqlList) {
////        Driver myDriver =  getUncacheDriver();
////        Driver myDriver =  this.getCacheDriver();
////        AsyncSession session = asyncDriver.asyncSession(sessionConfig);
////        AsyncSession session = this.driver.asyncSession(sessionConfig);
//
////        AsyncSession asyncSession =  asyncDriver.asyncSession(sessionConfig);
//
//        CompletableFuture.supplyAsync(() -> {
//            AsyncSession session = asyncDriver.asyncSession(sessionConfig);
//                return session.writeTransactionAsync(tx -> {
//                    List<CompletionStage<Void>> writeOperations = new ArrayList<>();
//                    for (String cql : cqlList) {
//                        CompletionStage<Void> writeOp = tx.runAsync(cql)
//                                .thenApply(ignore -> null); // 只关注操作完成，不返回结果
//                        writeOperations.add(writeOp);
//                    }
//                    return CompletableFuture.allOf(writeOperations.toArray(new CompletableFuture<?>[0]))
//                            .thenCompose((Void) -> CompletableFuture.supplyAsync(() -> {
//                                session.closeAsync();
//                                return null;
//                            }));
//                }).toCompletableFuture().join();
//
//        })
//                .handle((result, ex) -> {
//                    if (ex != null) {
//                        // 处理异常
////                        log.error("Error in async session", ex);
//                        failMsgManager.addMutilCql(cqlList);
//                        // 返回一个默认结果
//                        return null;
//                    }
//                    return result; // 返回正常结果
//                });
//    }
//
//
//    public void runAsyncList2(List<String> cqlList) {
////        Driver myDriver =  getUncacheDriver();
////        Driver myDriver =  this.getCacheDriver();
////        AsyncSession session = asyncDriver.asyncSession(sessionConfig);
////        AsyncSession session = this.driver.asyncSession(sessionConfig);
//
//        AsyncSession session =  asyncSessionPool.borrowSession();
//        session.writeTransactionAsync(tx -> {
//            List<CompletionStage<ResultCursor>> queryResults = new ArrayList<>();
//            for (String cql : cqlList) {
//                CompletionStage<ResultCursor> result = tx.runAsync(cql);
//                queryResults.add(result);
////                count.addAndGet(1);
//            }
//            CompletableFuture<?>[] queryResultsArray = queryResults.toArray(new CompletableFuture<?>[0]);
//            CompletableFuture.allOf(queryResultsArray)
//                    .thenAcceptAsync((Void) -> {
//                        // 处理结果
//                    })
//                    .exceptionally(ex -> {
//                        // 处理异常
//                        log.error("runAsyncList2 error!!!! "+cqlList, ex);
//                        failMsgManager.addMutilCql(cqlList);
//                        return null;
//                    })
//                    .thenRunAsync(() -> {
////                        session.closeAsync();  // 异步关闭 AsyncSession
////                        log.info("total write count "+ count.get());
////                        myDriver.closeAsync();
////                        this.returnCacheDriver(myDriver);
//                        asyncSessionPool.returnSession(session);
//
//                    });
//
////                    .thenComposeAsync((Void) -> session.closeAsync())
////                    .thenRun(() -> {
////                        // 任务执行完成后的逻辑
////
////                    });
//
//            return CompletableFuture.allOf(queryResultsArray);
//        });
//    }
//
//    private void handleException(Throwable ex, String cql) {
//        // 处理异常，并获取失败的CQL语句
////        log.error("执行失败的CQL语句: " + cql,ex );
////        ex.printStackTrace();
//        // 其他处理逻辑
//
//
//    }
//
//
//
////
////    public void runAsyncList2(AsyncSession session,List<String> cqlList) {
////        session.beginTransactionAsync()
////                .thenCompose(transaction -> {
////                    List<CompletionStage<ResultCursor>> queryResults = new ArrayList<>();
////                    for (String cql : cqlList) {
////                        CompletionStage<ResultCursor> result = transaction.runAsync(cql);
////                        queryResults.add(result);
////                    }
////                    return CompletableFuture.allOf(queryResults.toArray(new CompletableFuture[0]))
////                            .thenCompose(v -> transaction.commitAsync());
////                })
////                .whenComplete((ignored, error) -> {
////                    if (error != null) {
////                        // 处理错误
////                        log.error("runAsyncList2  error!",error);
////                    } else {
////                        // 事务成功提交
////                    }
////                    session.closeAsync(); // 关闭会话
////                });
////    }
//
//
//
//
////    public void executeQueryAsyncListTwo(List<String> queries)  {
////        List<CompletionStage<ResultCursor>> queryResults = new ArrayList<>();
////        AsyncSession session = null;
////        try {
////            session = sessionPool.borrowObject();
////            for (String query : queries) {
////                CompletionStage<ResultCursor> result = session.runAsync(query);
////                queryResults.add(result);
////            }
////
////            for (CompletionStage<ResultCursor> result : queryResults) {
////                result.thenComposeAsync(cursor -> processResultCursor(cursor));
////            }
////
////        }
////        catch (Exception ex)
////        {
////            log.error("executeQueryAsyncList  error!",ex);
////        }
////        finally {
////            try {
////                sessionPool.returnObject(session);
////            } catch (Exception e) {
////                e.printStackTrace();
////            }
////        }
////    }
//
//    public void close() {
//        sessionPool.close();
//    }
//
//    public static void main(String[] args) {
//        Neo4jAsyncDataRepository example = new Neo4jAsyncDataRepository();
//
//        try {
//            // 异步执行查询
//            example.executeQueryAsync("MATCH (n) RETURN n LIMIT 10");
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            example.close();
//        }
//    }
//}