package com.boulderai.metabase.etl.e.engine;

import com.boulderai.metabase.context.env.DefaultEnvironment;
import com.boulderai.metabase.context.env.EnvironmentDefinite;
import com.boulderai.metabase.etl.e.api.engine.ChangeEventConsumer;
import com.boulderai.metabase.etl.e.api.engine.ExtractEngine;
import com.boulderai.metabase.etl.e.api.engine.ExtractSourceType;
import com.boulderai.metabase.etl.e.api.engine.SourceTypeEnum;
import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.SyncPostgresConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.Neo4jSyncContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.PostgresConnectionManager;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.etl.tl.neo4j.util.DbOperateUtil;
import com.boulderai.metabase.etl.tl.neo4j.util.SpringApplicationContext;
import com.boulderai.metabase.lang.Constants;
import com.boulderai.metabase.lang.util.SleepUtil;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Component
public class PgExtractEngine implements ExtractEngine , ApplicationListener {

    private DebeziumEngine<ChangeEvent<String, String>> engine;

    @Autowired
    private List<ChangeEventConsumer> consumers;

    @Autowired
    private PgWalConfig pgWalConfig;

    private Random random=new Random();

    private AtomicBoolean streamStartFlag=new AtomicBoolean(false);

    private final Neo4jSyncContext neo4jSyncContext =Neo4jSyncContext.getInstance();

    @Resource
    private SyncPostgresConfig syncPostgresConfig;

    private volatile Boolean  needCreateNewSlot=false;

    private List<String>  schemaList=new ArrayList<>();

    private  List<String>   excludeTableList=new ArrayList<String>();

    private Pattern EXCLUDE_TABLE_PATTERN = Pattern.compile("\\d{4,}|copy\\d+|ver__backup");

    @Override
    public void closeEngine() {
        try {
            if(engine == null ){
                return;
            }
            engine.close();
            neo4jSyncContext.setExtractEngineRunning(false);
        } catch (IOException ignored) {
            log.error(ignored.getMessage());
        }
    }



    /**
     * 创建CDC引擎
     *
     *
     name=inventory-connector
     connector.class=io.debezium.connector.mysql.MySqlConnector
     database.hostname=118.190.209.102
     database.port=5700
     database.user=root
     database.password=123456
     database.server.id=129129
     database.server.name=debezium1  # debezium的database.server.name一定要和bireme的data_source保持一致
     database.whitelist=syncdb1  # 同步的数据库列表
     database.history.kafka.bootstrap.servers=localhost:9092
     database.history.kafka.topic=dbhistory.debezium1
     include.schema.changes=true
     *
     */
    @Override
    public void create() {

        if (pgWalConfig.getClearingNeo4j()) {
            log.info(" 正在清理neo4j数据库，请稍后 ");
            return;
        }

        List<String>   checkSchemaList= Arrays.asList( Constants.CHECK_DATA_CHANGE_SCEMA.split(","));

        EnvironmentDefinite env= DefaultEnvironment.getCurrentEnvironment();
        if (env!=null&&env.equals(EnvironmentDefinite.unit_test)) {
            log.info(" 单元测试环境此处不跑 ");
            return;
        }

        SleepUtil.sleepSecond(random.nextInt(5)+1);
        streamStartFlag.set(true);
        String slotName=pgWalConfig.getSlotName();

        String tempCurrentSlot=null;
        List<Map<String,Object>> slotMapList= DbOperateUtil.queryMapList(DataSynConstant.DIS_ACTIVE_SLOT_SQL,syncPostgresConfig.getDbName());
        if (CollectionUtils.isNotEmpty(slotMapList)) {
            String maxSlotName=null;

            Map<String,Object>  mapObj= DbOperateUtil.queryMap(DataSynConstant.MAX_SLOT_ID_SQL);
            if(MapUtils.isNotEmpty(mapObj)) {
                Long maxId = (Long) mapObj.get("maxId");
                maxSlotName = DataSynConstant.SLOT_NAME_MODEL + maxId;
            }
            for (Map<String, Object> slotInfo: slotMapList) {
                String tempSlot= (String) slotInfo.get("slot_name");
                if (tempSlot.equals(maxSlotName)) {
                    tempCurrentSlot=tempSlot;
                    break;
                }
            }

            if(tempCurrentSlot==null)
            {
                tempCurrentSlot= (String) slotMapList.get(0).get("slot_name");
            }

        }

        if(needCreateNewSlot)
        {
            needCreateNewSlot=false;
            slotName= createNewSlot();
        }
        else {
            if(tempCurrentSlot!=null)
            {
                slotName=tempCurrentSlot;
            }
            else
            {
                slotName=createNewSlot();
            }
        }
        log.info("Postgresql wal slot name is "+slotName+", sys will start ......");
        Properties properties = new Properties();

        excludeTableList.add("pg_cql_stat");
        excludeTableList.add("dataoperation_log");
        excludeTableList.add("sc_warehouse_period_snapshot");


        schemaList.add("public");
        schemaList.add("base");

        if (pgWalConfig.getWalFilterTable()) {
            String tableList= getTableWhiteList();
            // 同步上下文中添加对
            neo4jSyncContext.addWhiteTableList(Arrays.asList(tableList.split(",")));
            log.info("db TableWhiteList: "+tableList);
            if(StringUtils.isNotBlank(tableList))
            {
                properties.setProperty("table.include.list", tableList);
            }
        }
        // 获取当前工作目录
        String currentDir = System.getProperty("user.dir");
        log.info("dir:{}",currentDir);
        String offsetStorageFile = currentDir + File.separator + "offsets.dat";
        properties.setProperty("name", "metabase-connector");
        properties.put("database.server.name", "your_server_name");
        // 暂不依赖kafaka
//        propertiessetProperty("database.history.kafka.bootstrap.servers", "localhost:9092");
//        properties.setProperty("database.history.kafka.topic", "pg-write-neo4j");
        properties.setProperty("topic.prefix", "pg-write-neo4j");
        properties.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        properties.setProperty("database.history.file.filename", "dbhistory.dat");
        properties.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        // 替换nacos配置
        Pattern p = Pattern.compile("//(.*):(.*)/(.*)\\?");
        Matcher m = p.matcher(syncPostgresConfig.getUrl());
        String host = null;
        String port = null;
        String dbname = null;
        while (m.find()) {
            host = m.group(1);
            port = m.group(2);
            dbname = m.group(3);
        }
        properties.setProperty("database.hostname",host);
        properties.setProperty("database.port", port);
        properties.setProperty("database.dbname", dbname);
        properties.setProperty("database.user", syncPostgresConfig.getUsername());
        properties.setProperty("database.password", syncPostgresConfig.getPassword());
        properties.setProperty("database.server.id", "10001");
        properties.setProperty("include.schema.changes", "false");
        properties.setProperty("plugin.name", "pgoutput");
        // 不快照读
        properties.setProperty("snapshot.mode", "never");
        properties.setProperty("database.whitelist", "public");
        properties.setProperty("slot.name", slotName);
        properties.setProperty("schema.include.list", pgWalConfig.getWalSubscribeSchema());
        // 替换监听白名单
//        properties.setProperty("table.include.list", "master_data.md_custom_manage_organize");
        properties.setProperty("include.schema.changes", "false");
        properties.setProperty("heartbeat.interval.ms", "10000");
        properties.setProperty("offset.storage.file.filename", offsetStorageFile);
        properties.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        properties.setProperty("offset.flush.interval.ms", "60000");
        properties.setProperty("tasks.max", "3");
        properties.setProperty("debezium.source.schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        properties.setProperty("decimal.handling.mode","double");
        log.info("properties:{}", properties);

        List<ChangeEventConsumer> callConsumers =  consumers.stream().filter(consumer ->{
            ExtractSourceType sourceType = consumer.getClass().getDeclaredAnnotation(ExtractSourceType.class);
            if(sourceType == null || ! SourceTypeEnum.PG.equals(sourceType.type())){
                return false;
            }
            return true;
        }).collect(Collectors.toList());

        log.info("{} call consumer:{}","PG", callConsumers);

        engine = DebeziumEngine.create(Json.class)
                .using(properties)
                .notifying(event -> {
                    //业务逻辑，这里的value就是下述的debezium的数据格式
                    callConsumers.forEach(consumer ->{
                        consumer.consumer(event);
//                        log.info("PG {} received key {}     value:{}",consumer.getClass().getName(),event.key(), event.value());
                    });

                })
                .build();

        ExecutorService executor =  new ThreadPoolExecutor(3, 3,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());

        executor.execute(engine);
        neo4jSyncContext.setExtractEngineRunning(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> closeEngine()));

    }

    /**
     * Handle an application event.
     *
     * @param event the event to respond to
     */
    @Override
    public void onApplicationEvent(ApplicationEvent event) {
//        if (event instanceof ApplicationStartedEvent) {
//            create();
//        }else{
//            return;
//        }

    }

    public String  createNewSlot()
    {
        String dbName=null;
        Matcher matcher= DataSynConstant.JDBC_PATTERN.matcher(syncPostgresConfig.getUrl());
        if (matcher.find()) {
            dbName=matcher.group(1);
        }
        List<Map<String,Object>> slotMapList= DbOperateUtil.queryMapList(DataSynConstant.DIS_ACTIVE_SLOT_SQL,dbName);
        if(CollectionUtils.isNotEmpty(slotMapList)) {
            for (Map<String, Object> map: slotMapList) {
                String slotName= (String) map.get("slot_name");
                this.dropReplicationSlot(slotName);
//               String  sql=  String.format(DataSynConstant.DIS_ACTIVE_SLOT_SQL,slotName);
//               DbOperateUtil.queryMap(sql);
            }
        }
        Long maxId=   DbOperateUtil.insert(DataSynConstant.INSERT_SLOT_SQL,new Object[] { DataSynConstant.SLOT_NAME_MODEL  });
        String  slotName=DataSynConstant.SLOT_NAME_MODEL+maxId;
        String  sql=String.format(DataSynConstant.CREATE_SLOT_SQL,slotName);
        Boolean  res=  DbOperateUtil.queryTest(sql);
        if (res) {
            return slotName;
        }
        return null;
    }

    public void dropReplicationSlot(String slotName) {
        PostgresConnectionManager connectionManager =    SpringApplicationContext.getBean(PostgresConnectionManager.class);
        Connection conn=connectionManager.newRepConnection();
        try {
            PGConnection pgcon = conn.unwrap(PGConnection.class);
            pgcon.getReplicationAPI().dropReplicationSlot(slotName);
        } catch (Exception e) {
            log.error("dropReplicationSlot error!",e);
        }
    }

    public boolean isRun() {
        return !(engine == null);
    }

    private String getTableWhiteList()
    {
        if (StringUtils.isEmpty(pgWalConfig.getWalSubscribeSchema())) {
            return  null;
        }
        StringBuilder  tableList=new StringBuilder(" ");
        String[]  schemaList=pgWalConfig.getWalSubscribeSchema().split(",");
        for (int m=0;m<schemaList.length;m++) {
            String schema= schemaList[m];
            if("tp".equals(schema)|| "public".equals(schema)
                    || "master_data".equals(schema) || "base".equals(schema) )
            {
                List<Map<String, Object>>  tableInfos=  DbOperateUtil.queryMapList(DataSynConstant.PG_SUB_TABLES_SQL,schema);
                for (Map<String, Object> tableInfo : tableInfos) {
                    String   tableName= (String) tableInfo.get("table_name");
                    String   schemaName= (String) tableInfo.get("schema_name");
                    Integer subType= (Integer) tableInfo.get("sub_type");
                    Integer targetBussines= (Integer) tableInfo.get("target_bussines");
                    String disableSync =(String) tableInfo.get("disable_sync");
                    if(subType==0||subType==2&&!"1".equals(disableSync))
                    {
                        tableList.append(schemaName).append(".").append(tableName).append(",");
                        neo4jSyncContext.getTargetBussinessTableMap().put(tableName,targetBussines);
                    }
                    else
                    {
                        List<Map<String, Object>>  patterTableList=  DbOperateUtil.queryMapList(DataSynConstant.PG_PATTER_TABLE_SQL,schemaName,"%"+tableName.trim()+"%");
                        for (Map<String, Object>  patterMap: patterTableList) {
                            String   newName= (String) patterMap.get("new_name");
                            String   npiTableName= (String) patterMap.get("table_name");
                            tableList.append(newName).append(",");
                            neo4jSyncContext.getTargetBussinessTableMap().put(npiTableName,targetBussines);
                        }
                    }
                }
            }
            else
            {
                List<Map<String, Object>>  list=  DbOperateUtil.queryMapList(DataSynConstant.ALL_TABLE_IN_SCHEMA,schema);
                if(CollectionUtils.isNotEmpty(list))
                {
                    List<String>  realTableList=new ArrayList<String>();
                    for(Map<String, Object>  tableMap:list )
                    {
                        String tableName= (String) tableMap.get("table_name");
                        if(tableName.contains(Constants.VERSION_TABLE_SUB))
                        {
                            continue;
                        }
                        if(excludeTableList.contains(tableName))
                        {
                            continue;
                        }

                        Matcher matcher=    EXCLUDE_TABLE_PATTERN.matcher(tableName);
                        if (matcher.find()) {
                            continue;
                        }
                        realTableList.add(tableName);
                    }

                    int size=realTableList.size();
                    for (int k=0;k<size;k++) {
                        String tableName= realTableList.get(k);
                        tableList.append(schema).append(".").append(tableName).append(",");
                    }
                }


            }
        }

        tableList.deleteCharAt(tableList.length()-1);
        return  tableList.toString().trim();
    }
}
