package com.boulderai.metabase.etl.tl.neo4j.service.neo4j;


import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.CqlValueContainer;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.FailMsgManager;
import com.boulderai.metabase.etl.tl.neo4j.util.LogRecordCqlFlag;
import com.boulderai.metabase.etl.tl.neo4j.util.StartFlagger;
import com.boulderai.metabase.etl.tl.neo4j.util.Stopper;
import com.boulderai.metabase.etl.tl.neo4j.util.pool.neo4j.Neo4jSession;

import com.boulderai.metabase.etl.tl.neo4j.util.stat.CqlStat;
import com.boulderai.metabase.etl.tl.neo4j.util.stat.WebLogAspect;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * @author df.l
 * @ClassName: Neo4jOperaManager
 * @Description: neo4j操作
 * @date 2022年10月16日
 * @Copyright boulderaitech.com
 */
@Component
@Slf4j
public class Neo4jDataRepository implements INeo4jDataRepository {

    private final static Logger logger = LoggerFactory.getLogger(Neo4jDataRepository.class);

    //    @Autowired
//    private Neo4jConfig neo4jConfig;

//    private Driver neo4jDriver;

    @Autowired
    private WebLogAspect webLogAspect;

        @Autowired
    private Neo4jConfig neo4jConfig;

    private  Driver driver;

    @Autowired
    private FailMsgManager failMsgManager;

    private static final  String  MERGE_FLAG="MERGE";
    private static final  String  CREATE_FLAG="on CREATE";

//    private ReentrantLock  lock=new ReentrantLock();

    public void addCqlStat(String cql,Long useTime,String method,Integer  methodType)
    {
        if(!LogRecordCqlFlag.isStart())
        {
            return;
        }
        if(!Stopper.isRunning()|| !StartFlagger.isOK())
        {
            return;
        }
        CqlStat  cqlStat=new CqlStat();
        cqlStat.setUseTime(useTime);
        cqlStat.setCql(cql);
        cqlStat.setMethod(  method);
        cqlStat.setMethodType(methodType);
        webLogAspect.addCqlStat(cqlStat);
    }

    @Override
    @PostConstruct
    public void init() {

        Config config = Config.builder()
//                .withMaxConnectionLifetime(30, TimeUnit.MINUTES)
//                .withMaxConnectionPoolSize(50)
//                .withConnectionAcquisitionTimeout(2, TimeUnit.MINUTES)
                .withConnectionLivenessCheckTimeout( 60000, TimeUnit.MILLISECONDS )
                .build();

//        neo4jDriver = GraphDatabase.driver( neo4jConfig.getUri().trim(), AuthTokens.basic( neo4jConfig.getUsername().trim(),
//                neo4jConfig.getPassword().trim() ),config);
        this.driver = GraphDatabase.driver(neo4jConfig.getUri(), AuthTokens.basic(neo4jConfig.getUsername(), neo4jConfig.getPassword()),config);

    }

    /**
     * 操作neo4j失败处理
     *
     * @param cqlValueContainer
     */
    private void onDbOperFail(CqlValueContainer cqlValueContainer) {

    }


    public  long  queryCount(String cql,String countName) {
        Neo4jSession neo4jSession = Neo4jPoolManager.getInstance().getNeo4jSession();
        Session session = neo4jSession.getSession();
        long count=0L;
        try {
            Result result = session.run(cql);
            while (result.hasNext()) {
                Record record = result.next();
                Value countValue = record.get(countName);
                if (countValue != null) {
                    count = countValue.asLong();
                }
            }
        } catch (Exception ex) {
            logger.error("queryCount opera db fail! ", ex);
        } finally {
            Neo4jPoolManager.getInstance().closeNeo4jSession(neo4jSession);
        }
        return count;
    }

    public  Boolean queryTestNeo4j(String cql) {
        Neo4jSession neo4jSession = Neo4jPoolManager.getInstance().getNeo4jSession();
        Session session = neo4jSession.getSession();
        try {
             session.run(cql);
             return true;
        } catch (Exception ex) {
            logger.error("queryTestNeo4j opera db fail! ", ex);
            return false;
        } finally {
            Neo4jPoolManager.getInstance().closeNeo4jSession(neo4jSession);
        }
    }

    @Override
    public  List<Record> queryByCql(String cql) {
        Result result = null;
        Neo4jSession neo4jSession = Neo4jPoolManager.getInstance().getNeo4jSession();
        Session session = neo4jSession.getSession();
        List<Record> rlist = new ArrayList<Record>();
        try {
            result = session.run(cql);
            while (result != null && result.hasNext()) {
                Record record = result.next();
                rlist.add(record);
            }
        } catch (Exception ex) {
            logger.error("queryByCql neo4j opera db fail! ", ex);
        } finally {
            Neo4jPoolManager.getInstance().closeNeo4jSession(neo4jSession);
        }
        return rlist;
    }

    @Override
    public Boolean executeOneCql(List<String> cqlList) {
        Session session=this.driver.session();
        for (String cql : cqlList) {
            try {
                session.run(cql);
            } catch (Exception ex) {
                logger.error("executeOneCql  error! redo fail ",ex);
                failMsgManager.addOneCql(cql);
            }
            finally {

            }
        }
        if(session!=null)
        {
            session.close();
        }

        return true;
    }


//    public Boolean   executeAsyncOneCqlWithSimple(String cql ) {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        Session session=this.driver.session();
//        log.info(cql + " 执行异步cql： "+cql);
////        lock.lock();
//        try {
//
//        } catch (Exception ex) {
//            logger.error("executeAsyncOneCqlWithSimple  error! redo fail "+cql,ex);
//        }
//        finally {
////            lock.unlock();
//
//
//        }
//        stopWatch.stop();
//        long useTime=stopWatch.getTime();
//        log.info(cql + " 使用时间： "+useTime);
//        addCqlStat( cql, useTime,"executeOneCqlWithSimple",  0);
//        return true;
//    }


    @Override
    public Boolean   executeOneCqlWithSimple(String cql ) {
        StopWatch stopWatch=null;
        if(LogRecordCqlFlag.isStart())
        {
            stopWatch = new StopWatch();
            stopWatch.start();
        }

        Neo4jSession neo4jSession = Neo4jPoolManager.getInstance().getNeo4jSession();
        Session session =null;
        if(neo4jConfig.getDebugLog())
        {
            log.info("begin start exec cql:  "+cql);
        }

        try {
            session =neo4jSession.getSession();
            session.run(cql);
        } catch (Exception ex) {
            failMsgManager.addOneCql(cql);
            logger.error("executeOneCqlWithSimple  error! redo fail "+cql,ex);
        }
        finally {
            Neo4jPoolManager.getInstance().closeNeo4jSession(neo4jSession);
        }

        if(stopWatch!=null&&LogRecordCqlFlag.isStart())
        {
            stopWatch.stop();
            long useTime=stopWatch.getTime();
            addCqlStat( cql, useTime,"executeOneCqlWithSimple",  0);
            if(neo4jConfig.getDebugLog())
            {
                log.info(cql + " 使用时间： "+useTime);
            }
        }
        return true;
    }





    @Override
    public Boolean executeSmallBatchCql(List<String> cqlList) {
        Session session=this.driver.session();
        Transaction tx = session.beginTransaction();
        try {
            for (String cql : cqlList) {
                tx.run(cql);
            }
            tx.commit();
            return true;
        } catch (Exception ex) {
            logger.error("executeSmallBatchCql  error! redo fail ",ex);
        }
        finally {
            if(session!=null)
            {
                session.close();
            }
            return false;
        }

    }



    private void doExecuteBatchCql(List<String> cqlList)
    {
        StopWatch stopWatch=null;
        if(LogRecordCqlFlag.isStart())
        {
            stopWatch = new StopWatch();
            stopWatch.start();
        }
        Session session=this.driver.session();
        Transaction tx = session.beginTransaction();
        try {
            for (String cql : cqlList) {
                tx.run(cql);
            }
            tx.commit();
            session.close();
        } catch (Exception ex) {
//            tx.rollback();
            logger.error("executeBatchCql  error! redo fail cqls",ex);
            if(session!=null)
            {
                session.close();
            }
            doFailNodeRelation( cqlList);
//           throw ex;
        }
        finally {

        }

        if(stopWatch!=null&&LogRecordCqlFlag.isStart())
        {
            stopWatch.stop();
            long useTime=stopWatch.getTime();
            if(neo4jConfig.getDebugLog())
            {
                log.info(" handle cql size "+cqlList.size()+" use time "+useTime);
            }
        }

    }


        @Override
    public void executeBatchCql(List<String> cqlList) {
            List<String> nodeCql = new ArrayList<String>();
            List<String> relationCql = new ArrayList<String>();

            for (String cql : cqlList) {
                cql=cql.trim();
                //MERGE ( t:isc_shipping_order_header { isc_shipping_order_header_id:835482 })         on CREATE  set t=  {created_at:"2023-02-22 11:06:10",is_deleted:false,updated_at:"2023-06-04 21:54:15",system_version:0,is_lack_of_material:0,shipping_time:"2022-09-22 00:00:00",factory_code:"004.002",data_source:"K3",isc_shipping_order_header_id:835482,isc_shipping_order_no:"XOUT046356"}
                //	 on MATCH  set  t= {created_at:"2023-02-22 11:06:10",is_deleted:false,updated_at:"2023-06-04 21:54:15",system_version:0,is_lack_of_material:0,shipping_time:"2022-09-22 00:00:00",factory_code:"004.002",data_source:"K3",isc_shipping_order_header_id:835482,isc_shipping_order_no:"XOUT046356"}
                if (cql.startsWith(MERGE_FLAG)||cql.contains(CREATE_FLAG)) {
                    nodeCql.add(cql);
                } else {
                    relationCql.add(cql);
                }
            }
            if(!nodeCql.isEmpty())
            {
                doExecuteBatchCql( nodeCql);
            }
            if(!relationCql.isEmpty())
            {
//                doExecuteBatchCql( relationCql);
                for (String cql : cqlList) {
                    executeOneCqlWithSimple(cql );
//                    executeAsyncOneCqlWithSimple(cql );
                }

            }

    }

    private void doFailNodeRelation(List<String> cqlList)
    {
        List<String>  nodeCql=new ArrayList<String>();
        List<String>  relationCql=new ArrayList<String>();
        //节点和关系分离
        for (String cql : cqlList) {
            if (cql.contains(MERGE_FLAG)) {
                nodeCql.add(cql);
            }
            else {
                relationCql.add(cql);
            }
        }

        if (!nodeCql.isEmpty()) {
            //先保证节点先执行成功
            Session session=this.driver.session();
            Transaction tx = session.beginTransaction();
            try {
                for (String cql : nodeCql) {
                    tx.run(cql);
                }
                tx.commit();
            } catch (Exception ex) {
                logger.error("doFailNodeRelation  error! ",ex);
               Boolean bl= executeOneCql(nodeCql);
               if(!bl)
               {
                   failMsgManager.addMutilCql(nodeCql);
               }
            }
            finally {
                if(session!=null)
                {
                    session.close();
                }
            }
        }

        if(!relationCql.isEmpty())
        {
            Collections.shuffle(relationCql);
            failMsgManager.addMutilCql(relationCql);
        }
        nodeCql.clear();
        relationCql.clear();
    }

    @Override
    public  Result queryByCqlWithParameter(String cql, Value value) {
        Neo4jSession neo4jSession = Neo4jPoolManager.getInstance().getNeo4jSession();
        Session session = neo4jSession.getSession();
        Result result = null;
        try {
            result = session.run(cql, value);
        } catch (Exception ex) {
            logger.error("queryByCqlWithParameter neo4j opera db fail! ", ex);
        } finally {
//            closeSession(   session );
            Neo4jPoolManager.getInstance().closeNeo4jSession(neo4jSession);
        }
        return result;
    }

    /**
     * 通过cql 操作neo4j，批量处理
     *
     * @param cqlValueContainer 操作的具体内容，包括cql和值列表
     */
    @Override
    public  Boolean operNeo4jDataByCqlBatch(CqlValueContainer cqlValueContainer) {
        String cql = cqlValueContainer.getCql();
        LinkedList<Value> valueList = cqlValueContainer.getValueList();
        Boolean success = false;
        Neo4jSession neo4jSession = Neo4jPoolManager.getInstance().getNeo4jSession();
        Session session = neo4jSession.getSession();
        try {
            for (Value value : valueList) {
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                session.run(cql, value);
                stopWatch.stop();
                long useTime=stopWatch.getTime();
                addCqlStat( cql, useTime,"operNeo4jDataByCqlBatch",  0);
            }

            success = true;
        } catch (Exception ex) {
            onDbOperFail(cqlValueContainer);
            logger.error("operNeo4jDataByCql neo4j opera db fail! " + cqlValueContainer, ex);
            success = false;
        } finally {
            Neo4jPoolManager.getInstance().closeNeo4jSession(neo4jSession);
        }

        if (!success) {
            operNeo4jDataByCqlSingle(cqlValueContainer);
        }
        return true;
    }

    /**
     * 单条执行
     * @param cqlValueContainer
     * @return true
     */
    @Override
    public  Boolean operNeo4jDataByCqlSingle(CqlValueContainer cqlValueContainer) {
        Neo4jSession neo4jSession = Neo4jPoolManager.getInstance().getNeo4jSession();
        Session session = neo4jSession.getSession();
        String cql = cqlValueContainer.getCql();
        LinkedList<Value> valueList = cqlValueContainer.getValueList();
        for (Value value : valueList) {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            try {
                session.run(cql, value);
                log.info("元数据更新cql执行成功："+cql);
            } catch (Exception ex) {
                onDbOperFail(cqlValueContainer);

                if(ex.getMessage().contains("equivalent index already exists"))
                {
                }
                else {
                    logger.error("operNeo4jDataByCqlSingle  neo4j opera db fail! " + cqlValueContainer, ex);
                }
            } finally {
                stopWatch.stop();
                long useTime=stopWatch.getTime();
                addCqlStat( cql, useTime,"operNeo4jDataByCqlSingle",  1);

                Neo4jPoolManager.getInstance().closeNeo4jSession(neo4jSession);
            }

        }

        return true;
    }

    private void rollback(Transaction tx) {
        if (tx == null) {
            try {
                tx.rollback();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private void closeSession(Session session) {
        if (session != null) {
            try {
                session.close();
            } catch (Exception ex) {
//                ex.printStackTrace();
            }
        }
    }

    @Override
    public synchronized Boolean operNeo4jDataByCqlBatch(List<String> cqlList) {
        Boolean success = false;
        Neo4jSession neo4jSession = Neo4jPoolManager.getInstance().getNeo4jSession();
        Session session = neo4jSession.getSession();
        try {
            for (String cql : cqlList) {
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                session.run(cql);
                stopWatch.stop();
                long useTime=stopWatch.getTime();
                addCqlStat( cql, useTime,"operNeo4jDataByCqlBatch",  2);

            }
            success = true;
        } catch (Exception ex) {
            logger.error(" neo4j opera db  cql fail! " + cqlList, ex);
        } finally {
            Neo4jPoolManager.getInstance().closeNeo4jSession(neo4jSession);
        }

        if (!success) {
            for (String cql : cqlList) {
                operNeo4jDataByCqlSingle(cql);
            }
        }

        return true;
    }


    @Override
    public Boolean operNeo4jDataByCqlSingleNoExLog(String cql) {
        Neo4jSession neo4jSession = Neo4jPoolManager.getInstance().getNeo4jSession();
        Session session = neo4jSession.getSession();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try {
            session.run(cql);
        } catch (Exception ex) {
        } finally {
            stopWatch.stop();
            long useTime=stopWatch.getTime();
            addCqlStat( cql, useTime,"operNeo4jDataByCqlSingleNoExLog",  3);
            Neo4jPoolManager.getInstance().closeNeo4jSession(neo4jSession);
        }
        return true;
    }

    @Override
    public synchronized Boolean operNeo4jDataByCqlSingleNoEx(String cql,Boolean debugLog) {
        Neo4jSession neo4jSession = Neo4jPoolManager.getInstance().getNeo4jSession();
        Session session = neo4jSession.getSession();
//        Session session =null;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try {
            session.run(cql);
        } catch (Exception ex) {
            if(debugLog)//某些情况下可以忽略错误，如开始启动时候忽略创建已经存在索引的cql
            {
                logger.error(" neo4j operNeo4jDataByCqlSingleNoEx db  cql fail! " + cql, ex);
            }

        } finally {
            stopWatch.stop();
            long useTime=stopWatch.getTime();
            Neo4jPoolManager.getInstance().closeNeo4jSession(neo4jSession);
            addCqlStat( cql, useTime,"operNeo4jDataByCqlSingleNoEx",  4);
        }
        return true;
    }

    @Override
    public synchronized Boolean operNeo4jDataByCqlSingle(String cql) {
        Neo4jSession neo4jSession = Neo4jPoolManager.getInstance().getNeo4jSession();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        //日志太多了就打印一次，如果需要重试则修改大
        for (int k = 0; k < 2; k++) {
            Session session = neo4jSession.getSession();
            try {
                session.run(cql);
                log.info("元数据更新cql执行成功："+cql);
                break;
            } catch (Exception ex) {
                String exMsg=ex.getMessage();
               //多线程并发可能对同一node所有关系操作出现并发问题，重试
                if(exMsg.contains("ExclusiveLock"))
               {
                   //有些错误不需要记录，如constraint不存在去drop报错这种可以忽略
                   logger.error("operNeo4jDataByCqlSingle  neo4j opera db  cql fail! " + cql, ex);
                   SleepUtil.sleepMillisecond(2000 + Math.round(2000));
               }
                else if(exMsg.contains("equivalent index already exists"))
                {
                    //索引已经存在忽略日志
                    break;
                }
               else
               {
                   //有些错误不需要记录，如constraint不存在去drop报错这种可以忽略
                   logger.error("operNeo4jDataByCqlSingle  neo4j opera db  cql fail! " + cql, ex);
                   break;
               }

            }
        }
        stopWatch.stop();
        long useTime=stopWatch.getTime();
        addCqlStat( cql, useTime,"operNeo4jDataByCqlSingle",  5);
        Neo4jPoolManager.getInstance().closeNeo4jSession(neo4jSession);
        return true;
    }


//    public Session  createSession()
//    {
//        return neo4jDriver.session();
//    }

//    public void close()
//    {
//        if(neo4jDriver!=null)
//        {
//            neo4jDriver.close();
//        }
//        neo4jDriver=null;
//    }

//    @Override
//    public void setNeo4jConfig(Neo4jConfig neo4jConfig) {
//        this.neo4jConfig = neo4jConfig;
//    }

//    public Driver getNeo4jDriver() {
//        return neo4jDriver;
//    }
}
