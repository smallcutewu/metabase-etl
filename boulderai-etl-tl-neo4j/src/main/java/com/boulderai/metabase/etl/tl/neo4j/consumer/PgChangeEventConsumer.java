package com.boulderai.metabase.etl.tl.neo4j.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.boulderai.metabase.etl.e.api.engine.ChangeEventConsumer;
import com.boulderai.metabase.etl.e.api.engine.ExtractSourceType;
import com.boulderai.metabase.etl.e.api.engine.SourceTypeEnum;
import com.boulderai.metabase.etl.tl.neo4j.config.*;
import com.boulderai.metabase.etl.tl.neo4j.consumer.service.InterceptorStrategy;
import com.boulderai.metabase.etl.tl.neo4j.consumer.service.InterceptorStrategyFactory;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jPoolManager;
import com.boulderai.metabase.etl.tl.neo4j.service.pipeline.IProcessPipeline;
import com.boulderai.metabase.etl.tl.neo4j.service.pipeline.PipelineContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.*;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.EventType;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.StreamContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.WalChangeOldKey;
import com.boulderai.metabase.etl.tl.neo4j.util.*;
import com.boulderai.metabase.lang.Constants;
import com.google.gson.Gson;
import io.debezium.engine.ChangeEvent;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.DataSynTopics;

/**
 * @author wjw
 */
@ExtractSourceType(type = SourceTypeEnum.PG)
@Component("pgChangeEventConsumer")
public class PgChangeEventConsumer implements ChangeEventConsumer {

    private  final static Logger logger = LoggerFactory.getLogger(PgChangeEventConsumer.class);

    /**
     * 需要处理的table名称
     */
//    private ConcurrentHashMap<String,Integer>  enableTableMap=new ConcurrentHashMap<String,Integer>(64);

    private  static final Integer  DEFAULT_PROCESS_PIPELINE_COUNT=4;

    private final Map<Integer, IProcessPipeline> noe4jProcessPipelineMap =new ConcurrentHashMap<Integer,IProcessPipeline>(16);
//    private final Map<Integer,IProcessPipeline> pgProcessPipelineMap =new ConcurrentHashMap<Integer,IProcessPipeline>(16);


    private AtomicLong changeIdFlag=new AtomicLong(1L);
    private AtomicLong  threadId=new AtomicLong(1L);
    private AtomicInteger connectionFailFlag=new AtomicInteger(0);

    /**
     * Gson实例在调用Json操作时不保持任何状态,可以共享
     */
    private Gson gson = new Gson();

    private volatile String  lastReceiveLsn=null;

    private volatile Boolean  needCreateNewSlot=false;

    /**
     * 判断当前wal连接是否关闭或超时了
     */
    private AtomicBoolean pgWalRunning=new AtomicBoolean(false);

    private  static final  Integer MAX_LOOP=Integer.MAX_VALUE-20000;

    private AtomicInteger loopCount=new AtomicInteger(0);
    private volatile  Integer   lastLoopCountOk=0;
    private volatile  boolean   debug=false;

    @Autowired(required=false)
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitConfig rabbitConfig;

    @Resource
    private Neo4jConfig neo4jConfig;

    @Resource
    private SyncPostgresConfig syncPostgresConfig;

    /**
     * wal配置
     */
    @Autowired
    private PgWalConfig pgWalConfig;

    @Autowired
    private ZookeeperConfig zookeeperConfig ;

    @Autowired
    private MqMetabaseListenerConsumer mqMetabaseListenerConsumer;

    @Autowired
    private MqNpiModelDataListenerConsumer mqNpiModelDataListenerConsumer;

    @Autowired
    private MqIntegrationModelDataListenerConsumer mqIntegrationModelDataListenerConsumer;

    @Autowired
    private MqEimosModelDataListenerConsumer mqEimosModelDataListenerConsumer;

    @Autowired
    private MqClusterDataListenerConsumer mqClusterDataListenerConsumer;

    @Autowired
    private SpringApplicationContext springApplicationContext;

    @Autowired
    private PostgresConnectionManager postgresConnectionManager ;

    private MqIntergrationClusterConsumer mqIntergrationClusterConsumer;

    private ScheduledExecutorService executorService;
    private AtomicBoolean  streamStartFlag=new AtomicBoolean(false);


    private final Neo4jSyncContext neo4jSyncContext =Neo4jSyncContext.getInstance();
    private Integer  pipeWorkCount=4;
    //    private RabbitTemplate rabbitTemplate;
    private  volatile   Integer checkCount=1;
    private volatile StreamContext streamContext=new StreamContext();
    private Random random=new Random();

    private  static final  Integer TOO_BIG_SOCKET_BUFF_LENGTH=10000000;

    private List<String> schemaList=new ArrayList<>();

    private  List<String>   excludeTableList=new ArrayList<String>();

    private Pattern EXCLUDE_TABLE_PATTERN = Pattern.compile("\\d{4,}|copy\\d+|ver__backup");

    private String envName;

    private Map<String,Integer>  targetBussinessTableMap= neo4jSyncContext.getTargetBussinessTableMap();

    private volatile   long  lastFetchTime=0L;

    List<String>   checkSchemaList=Arrays.asList( Constants.CHECK_DATA_CHANGE_SCEMA.split(","));

    private Boolean enableTableSyn(String tableName)
    {
        return neo4jSyncContext.isTableEnableSynNew(tableName);
    }

    private Boolean enableModelDataSyn(String tableName)
    {
        return neo4jSyncContext.hasLogicEntityConfig(tableName);
    }

    @Autowired
    private InterceptorStrategyFactory interceptorStrategyFactory;
    @Override
    public boolean consumer(ChangeEvent<String, String> event) {
        PgWalChange change = convert2PgWalchange(event);
        if (change == null) {
            return true;
        }
//        change = interceptorStrategyFactory.getInterceptorStratrgy("filterChangeStrategy").doInterceptor(change);
        for (InterceptorStrategy interceptorStrategy : interceptorStrategyFactory.getInterceptorStrategyList()) {
            change = interceptorStrategy.doInterceptor(change);
        }
        handleEvent(change);
        return true;
    }

    /**
     * 上下文初始化
     */
    @PostConstruct
    private void checkRabbitTemplate() {
        if (rabbitTemplate==null) {
            CachingConnectionFactory connection = new CachingConnectionFactory();
            connection.setAddresses(rabbitConfig.getHost());
            connection.setUsername(rabbitConfig.getUsername());
            connection.setPassword(rabbitConfig.getPassword());
            connection.setVirtualHost("/");
            rabbitTemplate = new RabbitTemplate(connection);
        }
        String ip= NetUtils.getLocalAddress();
        if(StringUtils.isNotBlank(ip)&&ip.startsWith(DataSynConstant.IP_LOCAL_DEV_IP))
        {
//            pgWalConfig.setSlotName(DataSynConstant.SLOT_NAME_MATA_DEV);
//            zookeeperConfig.setMasterNodeName(DataSynConstant.ZK_NODE_MASTER_NAME);
            pgWalConfig.setMqQueue(pgWalConfig.getMqQueue() + "_dev");
            pgWalConfig.setMqExchange(pgWalConfig.getMqExchange()+"_dev");
            pgWalConfig.setMqRouting(pgWalConfig.getMqRouting() + "_dev");
        }
        excludeTableList.add("pg_cql_stat");
        excludeTableList.add("dataoperation_log");
        excludeTableList.add("sc_warehouse_period_snapshot");
        schemaList.add("public");
        schemaList.add("base");
        pipeWorkCount=pgWalConfig.getPipeWorkCount();
        if(pgWalConfig.getSaveIntegrationData())
        {
            IntegrationDataStopper.restart();
        }
        else
        {
            IntegrationDataStopper.stop();
        }
        postgresConnectionManager.setPostgresConfig(syncPostgresConfig);
        postgresConnectionManager.init();
        /**
         * 查询需要处理的表
         */

        mqMetabaseListenerConsumer.start();

        mqEimosModelDataListenerConsumer.start();

        mqIntegrationModelDataListenerConsumer.start();

        mqNpiModelDataListenerConsumer.start();

        mqClusterDataListenerConsumer.start();

        neo4jSyncContext.initData();
        PipelineContext.clear();
        Neo4jPoolManager.getInstance().setNeo4jConfig(neo4jConfig).init();

        if (pgWalConfig.getClearAllNeo4jOnStart()) {
            neo4jSyncContext.clearNeo4jData();
        }

        neo4jSyncContext.initTableConstraint();

        FileQueueManager.getInstance().setRabbitTemplate(rabbitTemplate);
        FileQueueManager.getInstance().start();
        String  fixConfigExTables[]=this.pgWalConfig.getExcludeTables().split(",");
        mqIntergrationClusterConsumer = SpringApplicationContext.getBean(MqIntergrationClusterConsumer.class);
        mqIntergrationClusterConsumer.setClusterMqStore(FileQueueManager.getInstance().getFileQueueStore(DataSynConstant.WAL_INTEGRATION_MODEL_DATA_QUEUE_NAME).clusterMqStore);
        mqIntergrationClusterConsumer.start();
        StartFlagger.restart();
        if (fixConfigExTables.length>0) {
            for (String  table: fixConfigExTables) {
                excludeTableList.add(table);
            }
        }

        for( IProcessPipeline pipeline:  noe4jProcessPipelineMap.values())
        {
            pipeline.start();
        }
        interceptorStrategyFactory.getInterceptorStrategyList().stream().forEach(interceptorStrategy -> interceptorStrategy.refreshContext());
//        EnvironmentDefinite env= DefaultEnvironment.getCurrentEnvironment();
//        if (env!=null&&env.equals(EnvironmentDefinite.unit_test)) {
//            logger.info(" 单元测试环境此处不跑 ");
//            return;
//        }

    }

    // todo 待验证能否用上



    private void  handleEvent(PgWalChange change )
    {

//        String tempCurrentSlot=null;
//        List<Map<String,Object>> slotMapList= DbOperateUtil.queryMapList(DataSynConstant.DIS_ACTIVE_SLOT_SQL,syncPostgresConfig.getDbName());
//        if (CollectionUtils.isNotEmpty(slotMapList)) {
//            String maxSlotName=null;
//
//            Map<String,Object>  mapObj= DbOperateUtil.queryMap(DataSynConstant.MAX_SLOT_ID_SQL);
//            if(MapUtils.isNotEmpty(mapObj)) {
//                Long maxId = (Long) mapObj.get("maxId");
//                maxSlotName = DataSynConstant.SLOT_NAME_MODEL + maxId;
//            }
//            for (Map<String, Object> slotInfo: slotMapList) {
//                String tempSlot= (String) slotInfo.get("slot_name");
//                if (tempSlot.equals(maxSlotName)) {
//                    tempCurrentSlot=tempSlot;
//                    break;
//                }
//            }
//
//            if(tempCurrentSlot==null)
//            {
//                tempCurrentSlot= (String) slotMapList.get(0).get("slot_name");
//            }
//
//        }

//        if(needCreateNewSlot)
//        {
//            needCreateNewSlot=false;
//            slotName= createNewSlot();
//        }
//        else {
//            if(tempCurrentSlot!=null)
//            {
//                slotName=tempCurrentSlot;
//            }
//            else
//            {
//                slotName=pgWalConfig.getSlotName();
//            }
//        }
//        long waitMin = pgWalConfig.getWaitMin();
        long logCount=1L;
        long MAX_LOG_COUNT=Long.MAX_VALUE-500000;
        FileQueueManager  fileQueueManager=FileQueueManager.getInstance();
        try {

            pgWalRunning.set(true);
//            logger.info("pg rep connection stream start ok ! slotName: "+slotName);
                /**
                 * json反序列优化
                 * Gson实例在调用Json操作时不保持任何状态。
                 * 所以，可以自由地重复使用同一个对象来进行多个Json序列化和反序列化操作。
                 */
//                PgWalChange change = gson.fromJson(data, PgWalChange.class);


                if(change!=null)
                {

                    String schema = change.getSchema();
                    String tableName = change.getTable();
                    if (debug) {
                        logger.info(schema+"."+tableName+" change debug #### ");
                    }
                    if (
                            checkSchemaList.contains(schema) && (
                                    !tableName.endsWith(Constants.VERSION_TABLE_SUB)
                                            || !tableName.equals(Constants.DATA_OPERATION_LOG_TABLE_NAME)
                            )

                    ) {
                        //neo4j同步,只处理已经配置的表
                        if(this.enableTableSyn(tableName)
    //                                        && tableList.contains(tableName)
                        )
                        {
                            if (debug) {
                                logger.info(schema+"."+tableName+"  MetabaseData   ==> "+change.toString());
                            }
                            else {
                                logger.info(schema+"."+tableName+"  MetabaseData   ### ");
                            }
                            fileQueueManager.sendProduct(change,DataSynConstant.WAL_META_QUEUE_NAME);
                        }
                        else
                        {
                            if (debug) {
                                logger.info(schema+"."+tableName+" MetabaseData not in config  @@@ ");
                            }
                        }

                        if(!schema.contains(Constants.NAMESPACE_CODE_ODS_DATA)&&
                                this.enableModelDataSyn(tableName)
    //                                                                           && tableList.contains(tableName)
                        )
                        {
                            if(debug&& logCount%200==0)
                            {
                                logger.info(schema+"."+tableName+" ModelData ==> ");
                            }
                            if(logCount>=MAX_LOG_COUNT)
                            {
                                logCount=0L;
                            }
                            Integer bussinessType=  targetBussinessTableMap.getOrDefault(tableName,-100);
                            switch (bussinessType)
                            {
                                case DataSynConstant.TARGET_BUSSINESS_TYPE_360:
                                    fileQueueManager.sendProduct(change,DataSynConstant.WAL_MODEL_DATA_QUEUE_NAME);
                                    break;
                                case DataSynConstant.TARGET_BUSSINESS_TYPE_OTHER:
                                    FileQueueManager.getInstance().sendProduct(change,DataSynConstant.WAL_OTHER_MODEL_DATA_QUEUE_NAME);
                                    break;
                                case DataSynConstant.TARGET_BUSSINESS_TYPE_METABASE:
                                    fileQueueManager.sendProduct(change,DataSynConstant.WAL_META_QUEUE_NAME);
                                    break;
                                case DataSynConstant.TARGET_BUSSINESS_TYPE_ALL:
                                    fileQueueManager.sendProduct(change,DataSynConstant.WAL_MODEL_DATA_QUEUE_NAME);
                                    fileQueueManager.sendProduct(change,DataSynConstant.WAL_OTHER_MODEL_DATA_QUEUE_NAME);
                                    break;
                            }
                            logCount++;
                        }
                        change.clear();

                            }
                }

        } catch (Exception e) {

            if( e!=null&&StringUtils.isNotBlank(e.getMessage())
            )
            {
                if(!e.getMessage().contains(DataSynConstant.PG_WAL_CONN_EXCEPTION_ERROR)
                        ||  e.getMessage().contains(DataSynConstant.PG_WAL_CONN_EXCEPTION_ERROR_2)
                        ||  e.getMessage().contains(DataSynConstant.PG_WAL_CONN_EXCEPTION_ERROR_4)
                )
                {
                    needCreateNewSlot=true;
                }
                else
                {
                    Matcher  matcher=     DataSynConstant.PATTERN.matcher(e.getMessage());
                    if (matcher.find()) {
                        needCreateNewSlot=true;
                    }
                }
            }

            pgWalRunning.set(false);
        }finally {
            pgWalRunning.set(false);
            streamStartFlag.set(false);

            if(streamContext!=null)
            {
                streamContext.close();
            }
        }
    }



    public void dropReplicationSlot(String slotName) {
        PostgresConnectionManager  connectionManager =    SpringApplicationContext.getBean(PostgresConnectionManager.class);
        Connection conn=connectionManager.newRepConnection();
        try {
            PGConnection pgcon = conn.unwrap(PGConnection.class);
            pgcon.getReplicationAPI().dropReplicationSlot(slotName);
        } catch (Exception e) {
            logger.error("dropReplicationSlot error!",e);
        }
    }

    private PgWalChange convert2PgWalchange(ChangeEvent event) {
        PgWalChange pgWalChange = new PgWalChange();
        WalChangeOldKey oldKey = new WalChangeOldKey();
        String eventkey = (String) event.key();
        String data = (String)event.value();
        if (StringUtils.isEmpty(data)) {
            return null;
        }
        try {
            if (data.contains(DataSynConstant.DEBEZIUMCHANGE_READ) || data.contains(DataSynConstant.DEBEZIUMCHANGE_HEARTBEAT)) {
                return null;
            }
            JSONObject change = JSON.parseObject(data);
            JSONObject keyObject = JSON.parseObject(eventkey);
            List<Object> keyFields = (List)((Map)keyObject.get(DataSynConstant.DEBEZIUMCHANGE_SCHEMA)).get(DataSynConstant.DEBEZIUMCHANGE_FIELDS);
            List<String> keyNames = keyFields.stream().map(filed -> ((Map)filed).get(DataSynConstant.DEBEZIUMCHANGE_FIELD).toString()).collect(Collectors.toList());
            List<String> keyTypes = keyFields.stream().map(filed -> translateType(filed)).collect(Collectors.toList());
            List<String> keyvalues = keyNames.stream().map(name -> ((Map)keyObject.get(DataSynConstant.DEBEZIUMCHANGE_PYLOAD)).get(name).toString()).collect(Collectors.toList());
            oldKey.setKeynames(keyNames);
            oldKey.setKeytypes(keyTypes);
            oldKey.setKeyvalues(keyvalues);
            pgWalChange.setOldkeys(oldKey);
            pgWalChange.setSchema(((Map)((Map) change.get(DataSynConstant.DEBEZIUMCHANGE_PYLOAD)).get(DataSynConstant.DEBEZIUMCHANGE_SOURCE)).get(DataSynConstant.DEBEZIUMCHANGE_SCHEMA).toString());
            pgWalChange.setTable(((Map)((Map) change.get(DataSynConstant.DEBEZIUMCHANGE_PYLOAD)).get(DataSynConstant.DEBEZIUMCHANGE_SOURCE)).get(DataSynConstant.DEBEZIUMCHANGE_TABLE).toString());
            if ("d".equals(((Map) change.get(DataSynConstant.DEBEZIUMCHANGE_PYLOAD)).get(DataSynConstant.DEBEZIUMCHANGE_OP).toString())) {
                pgWalChange.setKind(EventType.DELETE.toString());
                return pgWalChange;
            }
            Map<Object,Object> after = (Map)((Map) change.get(DataSynConstant.DEBEZIUMCHANGE_PYLOAD)).get(DataSynConstant.DEBEZIUMCHANGE_AFTER);
            Map<Object,Object> before = (Map)((Map) change.get(DataSynConstant.DEBEZIUMCHANGE_PYLOAD)).get(DataSynConstant.DEBEZIUMCHANGE_BEFORE);
            List<Object> fields = (List)((List) ((Map)change.get(DataSynConstant.DEBEZIUMCHANGE_SCHEMA)).get(DataSynConstant.DEBEZIUMCHANGE_FIELDS)).stream().filter(object -> !DataSynConstant.DEBEZIUMCHANGE_AFTER.equals(((Map)object).get(DataSynConstant.DEBEZIUMCHANGE_FIELD).toString())).collect(Collectors.toList());
            List<Object> typesFields = (List)((Map)fields.get(0)).get(DataSynConstant.DEBEZIUMCHANGE_FIELDS);
            List<String> columnNames = after.keySet().stream().map(key -> key.toString()).collect(Collectors.toList());
            List<String> columnValues = after.values().stream().map(value -> value== null? null:value.toString()).collect(Collectors.toList());
            List<String> types = buildSortedTypes(typesFields,columnNames);
            pgWalChange.setColumnnames(columnNames);
            pgWalChange.setColumnvalues(columnValues);
            pgWalChange.setColumntypes(types);
            pgWalChange.setBefore(before);
            pgWalChange.setAfter(after);
            switch (((Map) change.get(DataSynConstant.DEBEZIUMCHANGE_PYLOAD)).get(DataSynConstant.DEBEZIUMCHANGE_OP).toString()) {
                case "c" :
                    pgWalChange.setKind(EventType.INSERT.toString());
                    break;
                case "u" :
                    pgWalChange.setKind(EventType.UPDATE.toString());
                    break;
            }
        } catch (Exception e) {
            logger.error("数据变更change转换失败："+data);
            logger.error(e.getMessage());
        }
        return pgWalChange;
    }

    private List<String> buildSortedTypes(List<Object> typesFields,List<String> columnNames) {
        if (CollectionUtils.isEmpty(typesFields) || CollectionUtils.isEmpty(columnNames)) {
            return null;
        }
        List<String> types = new ArrayList<>();
        for (String columnName : columnNames) {
            for (Object object : typesFields) {
                 if (columnName.equals(((Map)object).get(DataSynConstant.DEBEZIUMCHANGE_FIELD))) {
                     types.add(translateType(object));
                 }
            }
        }
        return types;
    }

    private String translateType(Object object) {
        if (object == null) {
            return null;
        }
        String returnType = ((Map)object).get(DataSynConstant.DEBEZIUMCHANGE_TYPE).toString();
        // 适配wal2json类型
        switch (((Map)object).get(DataSynConstant.DEBEZIUMCHANGE_TYPE).toString()) {
            case "string" :
                returnType = "char";
                break;
            case "int64" :
                if (DataSynConstant.DEBEZIUMCHANGE_MICROTIMESTAMP.equals(((Map)object).get(DataSynConstant.DEBEZIUMCHANGE_NAME)) ||
                        DataSynConstant.DEBEZIUMCHANGE_MICROTIME.equals(((Map)object).get(DataSynConstant.DEBEZIUMCHANGE_NAME))) {
                    returnType = "timestamp";
                } else{
                    returnType = "int8";
                }
                break;
            case "int32" :
                returnType = "int4";
                break;
            case "int16" :
                returnType = "int2";
                break;
            case "float32" :
                returnType = "float4";
                break;
            case "float64" :
                returnType = "float8";
                break;
            case "double" :
                returnType = "float8";
                break;
            case "boolean":
                break;
            default:
                break;
        }
        return returnType;
    }

}
