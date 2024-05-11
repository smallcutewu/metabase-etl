package com.boulderai.metabase.etl.tl.neo4j.controller;

import cn.hutool.core.util.BooleanUtil;
import com.boulderai.metabase.etl.tl.neo4j.job.ConfigReloadJob;
import com.boulderai.metabase.etl.tl.neo4j.service.IPgDataSyncService;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.CearNeo4jDataThread;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.FileQueueManager;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.MsgDebugManager;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.Neo4jSyncContext;
import com.boulderai.metabase.etl.tl.neo4j.util.IntegrationDataStopper;
import com.boulderai.metabase.etl.tl.neo4j.util.LogRecordCqlFlag;
import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @ClassName: HelloController
 * @Description: 应用启动查看是否ok接口
 * @author  df.l
 * @date 2022年10月18日
 * @Copyright boulderaitech.com
 */
@RestController
public class SynHelloController {
    @Autowired
    private IPgDataSyncService pgDataSyncService;

    @Autowired
    private ConfigReloadJob configReloadJob;

    @GetMapping("/hello")
    public String hello()
    {
        return "Hello data sync "+ new DateTime().toString();
    }

    @GetMapping("/testWalContext")
    public String testMqAndPg()
    {
        return pgDataSyncService.testWalContext();
    }


    @GetMapping("/watchConsumeResult")
    public String watchConsumeResult()
    {
       return FileQueueManager.getInstance().printTotalPerfile();
    }

    @GetMapping("/createNewSlot")
    public String createNewSlot()
    {
        return pgDataSyncService.createNewSlot();
    }

    @GetMapping("/dropDisableSlot")
    public Boolean dropDisableSlot()
    {
        return pgDataSyncService.dropDisableSlot();
    }


    @GetMapping("/showWalConfig")
    public String showWalConfig()
    {
        return  pgDataSyncService.showWalConfig();
    }


    @PostMapping("/refreshData")
    public Boolean refreshData2Neo4j(@RequestParam(name="schema")   String schema, @RequestParam(name="tableName") String tableName,@RequestParam(name="dropNeo4jTable",defaultValue = "0")String dropNeo4jTable)
    {
        int dropNeo4jTableFlag= NumberUtils.toInt(dropNeo4jTable,0);
         return pgDataSyncService.refreshData2Neo4j( schema,  tableName, dropNeo4jTableFlag);
    }

    @PostMapping("/refreshData2Neo4j")
    public Boolean refreshData2Neo4j(@RequestParam(name="schema")   String schema, @RequestParam(name="tableName") String tableName)
    {
        return pgDataSyncService.refreshMetabaseData2Neo4j( schema,  tableName);
    }

    @GetMapping("/reInitTableBeanConfig")
    public Boolean reInitTableBeanConfig()
    {
         Neo4jSyncContext.getInstance().reInitTableBeanConfig(true);
         return true;
    }

    @GetMapping("/changeDebug")
    public Boolean changeDebug(@RequestParam(name="debug",defaultValue = "false")   String debug)
    {
        Boolean debugFlag= BooleanUtil.toBoolean(debug);
        return pgDataSyncService.changeDebug( debugFlag);
    }

    @GetMapping("/initTableConstraint")
    public Boolean initTableConstraint()
    {
        Neo4jSyncContext.getInstance().initTableConstraint();
        return true;
    }

    @GetMapping("/clearNeo4jData")
    public Boolean clearNeo4jData()
    {
        CearNeo4jDataThread  thread=new CearNeo4jDataThread();
        thread.start();
        return true;
    }


    @GetMapping("/checkNeo4jData")
    public String configReloadJob(@RequestParam(name="checkAll",defaultValue = "false")   String checkAll)
    {
        Boolean checkAllFlag= BooleanUtil.toBoolean(checkAll);
        new checkNeo4jDataThread(checkAllFlag).start();
        return  new DateTime().toString();
    }

    private class   checkNeo4jDataThread  extends Thread {
       private  Boolean checkAllFlag;
        public  checkNeo4jDataThread(Boolean checkAllFlag)
        {
            this.checkAllFlag=checkAllFlag;
        }
        @Override
        public void run() {
            if(checkAllFlag)
            {
                configReloadJob.forkUpdateNeo4jData();
            }
            else {
                configReloadJob.checkNeo4jData();
            }

        }
    }


    @GetMapping("/changeMqMsgDebug")
    public Boolean changeMqMsgDebug(@RequestParam(name="emptyConsumeMetaData",defaultValue = "false")   String emptyConsumeMetaData
                                    , @RequestParam(name="emptyConsumeModelData",defaultValue = "false")   String emptyConsumeModelData
          )
    {
        Boolean emptyConsumeMetaDataFlag= BooleanUtil.toBoolean(emptyConsumeMetaData);
        Boolean emptyConsumeModelDataFlag= BooleanUtil.toBoolean(emptyConsumeModelData);
        MsgDebugManager.getInstance().setEmptyConsumeMetaData(emptyConsumeMetaDataFlag);
        MsgDebugManager.getInstance().setEmptyConsumeModelData(emptyConsumeModelDataFlag);
        return true;
    }


    @GetMapping("/changeLogRecordCqlFlag")
    public Boolean changeLogRecordCqlFlag(@RequestParam(name="logRecordCql",defaultValue = "false")   String logRecordCql
    )
    {
        Boolean logRecordCqlFlag= BooleanUtil.toBoolean(logRecordCql);
        if(logRecordCqlFlag)
        {
            LogRecordCqlFlag.restart();
        }
        else
        {
            LogRecordCqlFlag.stop();
        }
        return true;
    }


    @GetMapping("/changeIntegrationDataStopFlag")
    public Boolean changeIntegrationDataStopFlag(@RequestParam(name="integrationDataStop",defaultValue = "false")   String integrationDataStop
    )
    {
        Boolean integrationDataStopFlag= BooleanUtil.toBoolean(integrationDataStop);
        if(integrationDataStopFlag)
        {
            IntegrationDataStopper.restart();
        }
        else
        {
            IntegrationDataStopper.stop();
        }
        return true;
    }

}
