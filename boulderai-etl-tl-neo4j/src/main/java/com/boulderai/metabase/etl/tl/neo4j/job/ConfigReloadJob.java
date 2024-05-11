package com.boulderai.metabase.etl.tl.neo4j.job;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.FileQueueManager;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.Neo4jDataChecker;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.Neo4jSyncContext;
import com.boulderai.metabase.etl.tl.neo4j.util.stat.PgWalHealthStat;
import com.boulderai.metabase.etl.tl.neo4j.util.DataBatchUpdateUtil;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ConfigReloadJob {

    @Autowired
    private Neo4jDataChecker neo4jDataChecker;

    @Scheduled(cron = "0 0 12,16,23 * * ?")
    public void reloadConfig()
    {
        Neo4jSyncContext.getInstance().initModelDataConfig();
    }

//    @Scheduled(cron = "0 0 22 * * ?")
//    public void reInitTableBeanConfig()
//    {
//        Neo4jSyncContext.getInstance().reInitTableBeanConfig(false);
//    }

    @Scheduled(cron = "0 59 23 * * ?")
    public void clearStatLog()
    {
        FileQueueManager.getInstance().clearStatLog();
    }

//    @Scheduled(cron = "0 30 23 * * ?")
//    public void initTableConstraint()
//    {
//        Neo4jSyncContext.getInstance().initTableConstraint();
//    }

    @Scheduled(cron = "0 0 3 * * ?")
    public void forkUpdateNeo4jData()
    {
        DataBatchUpdateUtil.clearPkMap();
        Boolean finished=false;
        log.info("Start forkUpdateNeo4jData "+ DateTime.now().toString());
        try
        {
            neo4jDataChecker.forkCheck(false);
            finished=true;
        }
        catch (Exception ex)
        {
            log.error("forkUpdateNeo4jData.startChecker()  error!",ex);
        }
        log.info("End forkUpdateNeo4jData��res is "+finished+"  ####  "+ DateTime.now().toString());

    }


    @Scheduled(cron = "0 1 0 * * ?")
    public void clearContext()
    {
        PgWalHealthStat.clear();
    }

    @Scheduled(cron = "0 1 10,12,14,16 * * ?")
    public void forkCheckDetaData()
    {
        Boolean finished=false;
        log.info("Start forkCheckDetaData "+ DateTime.now().toString());
        try
        {
            neo4jDataChecker.forkCheck(true);
            finished=true;
        }
        catch (Exception ex)
        {
            log.error("neo4jDataChecker.startChecker()  error!",ex);
        }
        log.info("End checkNeo4jData  res is "+finished+"  ####  "+ DateTime.now().toString());

    }


    @Scheduled(cron = "0 0 4,7,9,17,19,23 * * ?")
    public void checkNeo4jData()
    {
        Boolean finished=false;
        log.info("Start checkNeo4jData "+ DateTime.now().toString());
        try
        {
            neo4jDataChecker.startChecker();
            finished=true;
        }
        catch (Exception ex)
        {
           log.error("neo4jDataChecker.startChecker()  error!",ex);
        }
        log.info("End checkNeo4jData  res is "+finished+"  ####  "+ DateTime.now().toString());

    }




}
