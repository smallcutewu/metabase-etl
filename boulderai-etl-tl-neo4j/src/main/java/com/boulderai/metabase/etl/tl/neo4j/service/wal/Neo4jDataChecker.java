package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepository;
import com.boulderai.metabase.etl.tl.neo4j.util.*;
import com.boulderai.metabase.lang.number.Snowflake;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.job.CheckDataJobResBean;
//import com.boulderai.metabase.sync.core.util.DataBatchUpdateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


///**
// * @ClassName: Neo4jDataChecker
// * @Description: ���ݼ��
// * @author  df.l
// * @date 2023��06��18��
// * @Copyright boulderaitech.com
// */
@Component
@Slf4j
public class Neo4jDataChecker {

    public static final String   CHECK_NEO4J_IS_OK_CQL=" MATCH (n:Module) RETURN n.name limit  1 ";
    public static final String   CHECK_PG_IS_OK_CQL=" select count(1) as tempCount from public.da_logic_entity";

    public static final String   QUERY_TABLE_NEO4J_CQL=" MATCH ( n:%s ) RETURN count(n) ";

    public static final String   QUERY_TABLE_NEO4J_SQL=" select count(1) as tempCount from %s.%s ";

    public static final String   REMOVE_TABLE_FROM_NEO4J_CQL=" MATCH ( n:%s ) detach delete n ";

    public static final String   UPDATE_FAIL_TABLE_SQL=" update  %s.%s      SET updated_at = COALESCE(updated_at, current_timestamp) + INTERVAL '2 second'  ";

    public static final String   INSERT_CHECK_DATA_JOB_RES_SQL=" INSERT INTO system_core.check_data_job_res " +
            "       ( id, batch_id, schema_name, table_name, db_net_ok, neo4j_net_ok, pg_count, neo4j_count, check_type, check_result, create_time, update_time  ) " +
            "VALUES ( ?,  ?,         ?,            ?,         ?,            ?,           ?,         ?,         ?,          ?,            now(),         now()    ) ";


    @Autowired
    private PgWalConfig pgWalConfig;

    @Autowired
    private Neo4jDataRepository neo4jDataRepository;

    @Value("${spring.profiles.active}")
    private String envName;


    private Snowflake snowflake=new Snowflake();


    public void  forkCheck(Boolean detaData)
    {
        if(!pgWalConfig.getCheckPgNeo4jRecCount())
        {
            log.error("sys not config to check table record count in pg and neo4j !");
            return;
        }
        int checkType=detaData?1:2;
        boolean  dbOk=  DbOperateUtil.queryTest(CHECK_PG_IS_OK_CQL);
        boolean  neo4jOk=neo4jDataRepository.queryTestNeo4j(CHECK_NEO4J_IS_OK_CQL);
        if(dbOk&&neo4jOk)
        {
            long batchId=snowflake.nextId();
            List<Map<String, Object>> checkTableList=  DbOperateUtil.queryMapList(DataSynConstant.PG_CHECK_SUB_TABLES_SQL);
            for (Map<String, Object> tableInfo : checkTableList) {
                String tableName = (String) tableInfo.get("table_name");
                String schemaName = (String) tableInfo.get("schema_name");

                String cql= String.format(QUERY_TABLE_NEO4J_CQL,tableName);
                String sql= String.format(QUERY_TABLE_NEO4J_SQL,schemaName,tableName);
                int pgCount=  DbOperateUtil.getCount(sql);
                long neo4jCount= neo4jDataRepository.queryCount(cql,"count(n)");
                if(pgCount==neo4jCount)
                {
                    // id, batch_id, schema_name, table_name, db_net_ok, neo4j_net_ok, pg_count, neo4j_count, check_type, check_result, create_time
                    log.info(schemaName +"."+tableName+"  check table record ok! ");
                    Object[]  parameters= {
                            snowflake.nextId(),batchId,schemaName,tableName,true,true,pgCount,neo4jCount,checkType,1
                    };
                    DbOperateUtil.update(INSERT_CHECK_DATA_JOB_RES_SQL,parameters);
                }
                else
                {
                    dbOk=  DbOperateUtil.queryTest(CHECK_PG_IS_OK_CQL);
                    neo4jOk=neo4jDataRepository.queryTestNeo4j(CHECK_NEO4J_IS_OK_CQL);
                    if(dbOk&&neo4jOk&&pgCount>0)
                    {
                        Object[]  parameters= {
                                snowflake.nextId(),batchId,schemaName,tableName,true,true,pgCount,neo4jCount,checkType,2
                        };
                        DbOperateUtil.update(INSERT_CHECK_DATA_JOB_RES_SQL,parameters);
                        if(detaData)
                        {
                            //仅更新今天08：30之后的数据
                            DataBatchUpdateUtil.updateTableDeta(schemaName,tableName);
                        }
                        else
                        {
                            //更新全表数据，分页
                            DataBatchUpdateUtil.updateTablePaging(schemaName,tableName);
                        }

                    }

                }

            }
            sendMsg2Dingding( batchId);
        }


    }


    private void sendMsg2Dingding(long batchId)
    {
        if(!pgWalConfig.getDingdingNotify())
        {
            return;
        }
        List<CheckDataJobResBean>  checkTableList=  DbOperateUtil.queryBeanList(DataSynConstant.CHECK_DATA_JOB_RES_SQL, CheckDataJobResBean.class,batchId);
        if(CollectionUtils.isNotEmpty(checkTableList))
        {
            StringBuilder  sb=new StringBuilder("").append(batchId).append(" ").append(pgWalConfig.getRealProjectName());
            sb.append(" pg-neo4j data check result ").append(this.envName).append(" ").append(NetUtils.getLocalAddress()).append(" :  \\n");
            sb.append(" table_name  pg_count  neo4j_count  check_result   \\n");
            for (CheckDataJobResBean  map : checkTableList) {
                sb.append(map.getTableName()).append(" ").append(map.getPgCount()).append(" ").append(map.getNeo4jCount()).append(" ").
                        append(map.getCheckResult()).append(" \\n");
            }

            String context= sb.toString();
            List<String> dingDingMobiles=new ArrayList<>();
            DingDingUtil.sendDingDingMsg(context,dingDingMobiles);
        }
    }



    public void  startChecker()
    {
        if(!pgWalConfig.getCheckPgNeo4jRecCount())
        {
            log.error("sys not config to check table record count in pg and neo4j !");
            return;
        }
        boolean  dbOk=  DbOperateUtil.queryTest(CHECK_PG_IS_OK_CQL);
        boolean  neo4jOk=neo4jDataRepository.queryTestNeo4j(CHECK_NEO4J_IS_OK_CQL);
        if(dbOk&&neo4jOk)
        {
            int checkType=2;
            long batchId=snowflake.nextId();
            List<Map<String, Object>> checkTableList=  DbOperateUtil.queryMapList(DataSynConstant.PG_CHECK_SUB_TABLES_SQL);
            for (Map<String, Object> tableInfo : checkTableList) {
                String tableName = (String) tableInfo.get("table_name");
                String schemaName = (String) tableInfo.get("schema_name");

                String cql= String.format(QUERY_TABLE_NEO4J_CQL,tableName);
                String sql= String.format(QUERY_TABLE_NEO4J_SQL,schemaName,tableName);
                int pgCount=  DbOperateUtil.getCount(sql);
                long neo4jCount= neo4jDataRepository.queryCount(cql,"count(n)");
                if(pgCount==neo4jCount)
                {
                    log.info(schemaName +"."+tableName+"  check table record ok! ");

                    Object[]  parameters= {
                            snowflake.nextId(),batchId,schemaName,tableName,true,true,pgCount,neo4jCount,checkType,2
                    };
                    DbOperateUtil.update(INSERT_CHECK_DATA_JOB_RES_SQL,parameters);
//                    DataBatchUpdateUtil.updateTablePaging(schemaName,tableName);
                }
                else
                {
                    log.info(schemaName +"."+tableName+"  check table record  not ok! pgCount:"+pgCount+"  ;  neo4jCount:"+neo4jCount);

                    dbOk=  DbOperateUtil.queryTest(CHECK_PG_IS_OK_CQL);
                    neo4jOk=neo4jDataRepository.queryTestNeo4j(CHECK_NEO4J_IS_OK_CQL);
                    if(dbOk&&neo4jOk&&pgCount>0)
                    {
                        if(pgCount>neo4jCount)
                        {
                            Boolean canUpdateSql=  Math.round((pgCount-neo4jCount)*100/pgCount)>pgWalConfig.getMaxNeo4jDelayPercent();
                            if (canUpdateSql) {
//                                String updateCql= String.format(UPDATE_FAIL_TABLE_SQL,schemaName,tableName);
//                                DbOperateUtil.update(updateCql);
                                DataBatchUpdateUtil.updateTablePaging(schemaName,tableName);
                                log.info("job sleep 30 sec! "+tableName);

                                Object[]  parameters= {
                                        snowflake.nextId(),batchId,schemaName,tableName,true,true,pgCount,neo4jCount,checkType,2
                                };
                                DbOperateUtil.update(INSERT_CHECK_DATA_JOB_RES_SQL,parameters);
                                SleepUtil.sleepSecond(30);

                            }
                        }
                        else
                        {
                            if(neo4jCount-pgCount>=pgWalConfig.getMaxNeo4jDelayCount())
                            {
                                Object[]  parameters= {
                                        snowflake.nextId(),batchId,schemaName,tableName,true,true,pgCount,neo4jCount,checkType,2
                                };
                                DbOperateUtil.update(INSERT_CHECK_DATA_JOB_RES_SQL,parameters);

                                log.info("clear data from neo4j "+tableName);
                                String reCql= String.format(REMOVE_TABLE_FROM_NEO4J_CQL,tableName);
                                neo4jDataRepository.operNeo4jDataByCqlSingle(reCql);
                                SleepUtil.sleepSecond(30);
//                                String updateCql= String.format(UPDATE_FAIL_TABLE_SQL,schemaName,tableName);
//                                DbOperateUtil.update(updateCql);
//                                log.info("redo sql for sync : "+updateCql);
                                DataBatchUpdateUtil.updateTablePaging(schemaName,tableName);

                            }

                        }


                    }
                }

            }
            sendMsg2Dingding( batchId);
        }
        else {
            if(!dbOk)
            {
                log.error("Postgres db server has error!");
            }
            else
            {
                log.error("Neo4j db server has error!");
            }
        }

    }
}
