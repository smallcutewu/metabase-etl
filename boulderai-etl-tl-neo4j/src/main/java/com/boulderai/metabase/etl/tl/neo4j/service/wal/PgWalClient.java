package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.context.env.DefaultEnvironment;
import com.boulderai.metabase.context.env.EnvironmentDefinite;
import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jPoolManager;
import com.boulderai.metabase.etl.tl.neo4j.service.pipeline.IProcessPipeline;
import com.boulderai.metabase.etl.tl.neo4j.service.pipeline.PipelineContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWal2JsonRecord;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.StreamContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.queue.WalLsnManager;
import com.boulderai.metabase.etl.tl.neo4j.util.*;
import com.boulderai.metabase.lang.Constants;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.SyncPostgresConfig;
import com.boulderai.metabase.etl.tl.neo4j.util.stat.PgWalHealthStat;
import com.google.gson.Gson;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @ClassName: PgWalClient
 * @Description: pg主从复制连接客户端
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */


public class PgWalClient {

    private  final static Logger logger = LoggerFactory.getLogger(PgWalClient.class);

    /**
     * 需要处理的table名称
     */
//    private ConcurrentHashMap<String,Integer>  enableTableMap=new ConcurrentHashMap<String,Integer>(64);

    private  static final Integer  DEFAULT_PROCESS_PIPELINE_COUNT=4;

    private final Map<Integer, IProcessPipeline> noe4jProcessPipelineMap =new ConcurrentHashMap<Integer,IProcessPipeline>(16);
//    private final Map<Integer,IProcessPipeline> pgProcessPipelineMap =new ConcurrentHashMap<Integer,IProcessPipeline>(16);


    private AtomicLong  changeIdFlag=new AtomicLong(1L);
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
    private    AtomicBoolean  pgWalRunning=new AtomicBoolean(false);

    private  static final  Integer MAX_LOOP=Integer.MAX_VALUE-20000;

    private AtomicInteger loopCount=new AtomicInteger(0);
    private volatile  Integer   lastLoopCountOk=0;
    private volatile  boolean   debug=false;

    /**
     * wal配置
     */
    private  RabbitTemplate  rabbitTemplate;
    private PgWalConfig pgWalConfig;
    private  SyncPostgresConfig syncPostgresConfig;
    private Neo4jConfig neo4jConfig;

    private    ScheduledExecutorService executorService;
    private AtomicBoolean  streamStartFlag=new AtomicBoolean(false);


    private final   Neo4jSyncContext   neo4jSyncContext =Neo4jSyncContext.getInstance();
    private Integer  pipeWorkCount=4;
    //    private RabbitTemplate rabbitTemplate;
    private  volatile   Integer checkCount=1;
    private volatile StreamContext streamContext=new StreamContext();
    private Random random=new Random();

    private  static final  Integer TOO_BIG_SOCKET_BUFF_LENGTH=10000000;

    private List<String>  schemaList=new ArrayList<>();

    private  List<String>   excludeTableList=new ArrayList<String>();

    private Pattern EXCLUDE_TABLE_PATTERN = Pattern.compile("\\d{4,}|copy\\d+|ver__backup");

    private String envName;

    private Map<String,Integer>  targetBussinessTableMap=new HashMap<>(100);

    private volatile   long  lastFetchTime=0L;



    private Boolean enableTableSyn(String tableName)
    {
        return neo4jSyncContext.isTableEnableSynNew(tableName);
    }

    private Boolean enableModelDataSyn(String tableName)
    {
        return neo4jSyncContext.hasLogicEntityConfig(tableName);
    }

    public  PgWalClient(PgWalConfig pgWalConfig, Neo4jConfig neo4jConfig, SyncPostgresConfig syncPostgresConfig)
    {
        excludeTableList.add("pg_cql_stat");
        excludeTableList.add("dataoperation_log");
        excludeTableList.add("sc_warehouse_period_snapshot");


        schemaList.add("public");
        schemaList.add("base");
        this.syncPostgresConfig=syncPostgresConfig;
        this.pgWalConfig=pgWalConfig;
        this.neo4jConfig=neo4jConfig;
//        pgWalConfig.setPipeWorkCount(1);
        pipeWorkCount=pgWalConfig.getPipeWorkCount();

        if(pgWalConfig.getSaveIntegrationData())
        {
            IntegrationDataStopper.restart();
        }
        else
        {
            IntegrationDataStopper.stop();
        }

//        for(int i=0;i<pipeWorkCount;i++)
//        {
//            MqMetabase2Neo4jProcessPipeline pipeline=new MqMetabase2Neo4jProcessPipeline(i);
//            pipeline.setNeo4jConfig(neo4jConfig);
//            pipeline.addHandlers();
//            noe4jProcessPipelineMap.put(i,pipeline);
//        }

    }

    /**
     * 停止pgke客户端
     */
    public void close()
    {
        pgWalRunning.set(false);
//        Stopper.stop();
        if(executorService!=null)
        {
            executorService.shutdownNow();
        }
        executorService=null;
    }

    public Boolean createReplicationSlot(String slotName) {
        PostgresConnectionManager  connectionManager =    SpringApplicationContext.getBean(PostgresConnectionManager.class);
        Connection  conn=connectionManager.newRepConnection();
        try {
            PGConnection pgcon = conn.unwrap(PGConnection.class);
            pgcon.getReplicationAPI().createReplicationSlot().logical().withSlotName(slotName).withOutputPlugin("wal2json").make();
            return true;
        } catch (Exception e) {
            logger.error("createReplicationSlot  error!",e);
        }
        return false;
    }

    public void dropReplicationSlot(String slotName) {
        PostgresConnectionManager  connectionManager =    SpringApplicationContext.getBean(PostgresConnectionManager.class);
        Connection  conn=connectionManager.newRepConnection();
        try {
            PGConnection pgcon = conn.unwrap(PGConnection.class);
            pgcon.getReplicationAPI().dropReplicationSlot(slotName);
        } catch (Exception e) {
            logger.error("dropReplicationSlot error!",e);
        }
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
            || "master_data".equals(schema) )
            {
                List<Map<String, Object>>  tableInfos=  DbOperateUtil.queryMapList(DataSynConstant.PG_SUB_TABLES_SQL,schema);
                for (Map<String, Object> tableInfo : tableInfos) {
                    String   tableName= (String) tableInfo.get("table_name");
                    String   schemaName= (String) tableInfo.get("schema_name");
                    Integer subType= (Integer) tableInfo.get("sub_type");
                    Integer targetBussines= (Integer) tableInfo.get("target_bussines");
                    if(subType==0||subType==2)
                    {
                        tableList.append(schemaName).append(".").append(tableName).append(",");
                        targetBussinessTableMap.put(tableName,targetBussines);
                    }
                    else
                    {
                        List<Map<String, Object>>  patterTableList=  DbOperateUtil.queryMapList(DataSynConstant.PG_PATTER_TABLE_SQL,schemaName,"%"+tableName.trim()+"%");
                        for (Map<String, Object>  patterMap: patterTableList) {
                            String   newName= (String) patterMap.get("new_name");
                            String   npiTableName= (String) patterMap.get("table_name");
                            tableList.append(newName).append(",");
                            targetBussinessTableMap.put(npiTableName,targetBussines);
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

                        Matcher  matcher=    EXCLUDE_TABLE_PATTERN.matcher(tableName);
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




    /**
     * 创建pg 连接的变化流侦听
     * @param slotName
     */
    public PGReplicationStream getReplicationStream(String slotName) throws SQLException {
        PostgresConnectionManager  connectionManager =    SpringApplicationContext.getBean(PostgresConnectionManager.class);
        Connection  conn=connectionManager.newRepConnection();
        PGConnection pgConnection = conn.unwrap(PGConnection.class);


        /**
         * 创建连接Stream
         */
        ChainedLogicalStreamBuilder builder = pgConnection.getReplicationAPI().replicationStream().logical().withSlotName(slotName)
                .withSlotOption("include-lsn", true)
//                .withSlotOption("batch_size", String.valueOf(pgWalConfig.getWalBatchSize()))
//                .withSlotOption("format-version","2")
//                .withSlotOption("write-in-chunks","1")
                .withStatusInterval(20, TimeUnit.SECONDS);
        if (pgWalConfig.getWalFilterTable()) {
            String tableList= getTableWhiteList();
            logger.info("db TableWhiteList: "+tableList);
            if(StringUtils.isNotBlank(tableList))
            {
                builder.withSlotOption("add-tables",tableList );
            }

        }
//        Map<String,Object>  slotMap=  DbOperateUtil.queryMap(" select last_receive_lsn from pg_wal_last_receive_lsn where slot_name='"+slotName+"' ORDER BY update_time desc limit 1 ");
//        String    fromLsn=null;
//        if(MapUtils.isNotEmpty(slotMap))
//        {
////            fromLsn= (String) slotMap.get("last_receive_lsn");
//        }
//        if (StringUtils.isNotEmpty(fromLsn)){
//            builder.withStartPosition(LogSequenceNumber.valueOf(fromLsn));
//        }


        PGReplicationStream  stream= builder.start();
        if (stream!=null&&!stream.isClosed()) {
            if(streamContext!=null)
            {
                streamContext.close();
            }
            streamContext.setStream(stream);
            streamContext.setPgConnection(pgConnection);
            streamContext.setJdbcConnection(conn);
        }
        return stream;
    }




    /**
     * 启动pg 连接侦听
     */
    public void start() {
        /**
         * 查询需要处理的表
         */

        MqMetabaseListenerConsumer mqMetabaseListenerConsumer= SpringApplicationContext.getBean(MqMetabaseListenerConsumer.class);
        mqMetabaseListenerConsumer.start();

        MqEimosModelDataListenerConsumer mqEimosModelDataListenerConsumer= SpringApplicationContext.getBean(MqEimosModelDataListenerConsumer.class);
        mqEimosModelDataListenerConsumer.start();

        MqIntegrationModelDataListenerConsumer mqIntegrationModelDataListenerConsumer= SpringApplicationContext.getBean(MqIntegrationModelDataListenerConsumer.class);
        mqIntegrationModelDataListenerConsumer.start();

        MqNpiModelDataListenerConsumer mqNpiModelDataListenerConsumer= SpringApplicationContext.getBean(MqNpiModelDataListenerConsumer.class);
        mqNpiModelDataListenerConsumer.start();


//        MqFailMsgDataListenerConsumer mqFailMsgDataListenerConsumer= SpringApplicationContext.getBean(MqFailMsgDataListenerConsumer.class);
//        mqFailMsgDataListenerConsumer.start();

        neo4jSyncContext.initData();
        PipelineContext.clear();
        Neo4jPoolManager.getInstance().setNeo4jConfig(neo4jConfig).init();

        if (pgWalConfig.getClearAllNeo4jOnStart()) {
            neo4jSyncContext.clearNeo4jData();
        }

        neo4jSyncContext.initTableConstraint();

        FileQueueManager.getInstance().setRabbitTemplate(rabbitTemplate);
        FileQueueManager.getInstance().start();

        String slotName=pgWalConfig.getSlotName();
        WalLsnManager.getInstance().setSlotName(   slotName).init();

        String  fixConfigExTables[]=this.pgWalConfig.getExcludeTables().split(",");
        if (fixConfigExTables.length>0) {
            for (String  table: fixConfigExTables) {
                excludeTableList.add(table);
            }
        }

        EnvironmentDefinite env= DefaultEnvironment.getCurrentEnvironment();
        if (env!=null&&env.equals(EnvironmentDefinite.unit_test)) {
            logger.info(" 单元测试环境此处不跑 ");
            return;
        }

        StartFlagger.restart();

        /**
         * 创建侦听线程
         */
        createWalStreamHanldeThread();


        /**
         * 检查已经创建的数据库侦听线程是否断开或超时
         */
        executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PgWalClientConnCheck");
            // 设置线程为守护线程，主线程退出，子线程也随之退出
            t.setDaemon(true);
            return t;
        });
        executorService.scheduleWithFixedDelay(new CheckPgWalConnectionRunnable(),3,4,TimeUnit.MINUTES);
        for( IProcessPipeline pipeline:  noe4jProcessPipelineMap.values())
        {
            pipeline.start();
        }

    }

    public void createWalStreamHanldeThread( )
    {
        if( Stopper.isRunning()&& !streamStartFlag.get())
        {
            createWalThread();
        }
    }


    public void foreCreateWalStreamHanlde()
    {
        long duringTime=(System.currentTimeMillis()-lastFetchTime)/1000;
        if (duringTime>=60*6) {
            pgWalRunning.set(false);
            streamStartFlag.set(false);
            needCreateNewSlot=true;
            if(streamContext!=null)
            {
                streamContext.close();
            }
            createWalThread();

        }
    }

    private void   createWalThread()
    {
        Thread   streamThread=new Thread(new WalStreamHandleRunnable( ));
        streamThread.setDaemon(true);
        long tid=  threadId.incrementAndGet();
        streamThread.setName("WalStreamHandleRunnable-"+tid);
        streamThread.start();
    }

    public void changeDebug(Boolean debugFlag) {
        this.debug=debugFlag;
        logger.info("pg client debug flag is "+debugFlag);
    }

    /**
     * 检查wal连接是否ok线程
     */
    protected class  CheckPgWalConnectionRunnable implements  Runnable
    {
        @Override
        public void run() {
            if(!pgWalRunning.get())
            {
                createWalStreamHanldeThread();
                int failCount=connectionFailFlag.incrementAndGet();
                logger.error(" PgWalConnection  listenning error! System has ctreate new thread! failCount="+failCount);
            }
            else
            {
                if(checkCount>=300000)
                {
                    checkCount=0;
                }
                if(checkCount%2==0)
                {
                    logger.info(" PgWalConnection  listenning ok!  "+checkCount);
                }
            }
            checkCount++;


            int currentCount=loopCount.get();
            if(currentCount>0&&lastLoopCountOk==currentCount)
            {
                closeStreamContext();
            }

            lastLoopCountOk=currentCount;
        }
    }

    public void closeStreamContext()
    {
        if (streamContext!=null) {
            streamContext.close();
            needCreateNewSlot=true;
        }
    }



    public Boolean  dropDisableSlot()
    {
        List<Map<String,Object>> slotMapList= DbOperateUtil.queryMapList(DataSynConstant.DIS_ACTIVE_SLOT_SQL,syncPostgresConfig.getDbName());
        if(CollectionUtils.isNotEmpty(slotMapList)) {
            for (Map<String, Object> map: slotMapList) {
                String slotName= (String) map.get("slot_name");
                this.dropReplicationSlot(slotName);
            }
        }
        return true;
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


    /**
     * wal连接侦听线程
     */
    private class WalStreamHandleRunnable implements     Runnable
    {

        public WalStreamHandleRunnable()
        {
        }




        @Override
        public void run()
        {

            if (pgWalConfig.getClearingNeo4j()) {
                logger.info(" 正在清理neo4j数据库，请稍后 ");
                return;
            }

            List<String>   checkSchemaList=Arrays.asList( Constants.CHECK_DATA_CHANGE_SCEMA.split(","));

            EnvironmentDefinite env= DefaultEnvironment.getCurrentEnvironment();
            if (env!=null&&env.equals(EnvironmentDefinite.unit_test)) {
                logger.info(" 单元测试环境此处不跑 ");
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
                    slotName=pgWalConfig.getSlotName();
                }
            }

            logger.info("Postgresql wal slot name is "+slotName+", sys will start ......");
            long waitMin = pgWalConfig.getWaitMin();
            long logCount=1L;
            long MAX_LOG_COUNT=Long.MAX_VALUE-500000;
            FileQueueManager  fileQueueManager=FileQueueManager.getInstance();
            //创建侦听流
            PGReplicationStream  stream = null;
            try {
                stream = PgWalClient.this.getReplicationStream(slotName);

                pgWalRunning.set(true);
                logger.info("pg rep connection stream start ok ! slotName: "+slotName);
                while (Stopper.isRunning()&&stream!=null&& !stream.isClosed()) {
                    ByteBuffer msg = stream.readPending();
                    long count=loopCount.get();
                    if (count %150==0) {
                        PgWalHealthStat.makeHealth();
                        logger.info("PGRepStream aliving now "+count);
                    }
                    if (count>MAX_LOOP) {
                        loopCount.set(0);
                    }
                    loopCount.incrementAndGet();
                    lastFetchTime=System.currentTimeMillis();

                    if (msg == null) {
                        SleepUtil.sleepMillisecond(waitMin);
                        continue;
                    }

//                    if(StringUtils.isNotBlank(lastReceiveLsn))
//                    {
//                        WalLsnManager.getInstance().setLastLsn(lastReceiveLsn);
 //                       lastReceiveLsn=null;
//                    }

                    /**
                     * 读取消息head body，生成字符消息
                     */
                    int offset = msg.arrayOffset();
                    byte[] source = msg.array();
                    int length = source.length - offset;



                    String data =null;
                    //这里如果数据变化有百万条记录时候这个string可能会超过容量范围报错，只能通过记录错误全量同步
//                    try{
                        data =  new String(source, offset, length);
//                    }
//                    catch (Exception ex)
//                    {
//                        logger.error("byte to string transform error! length="+length,ex);
//                    }
                    if (debug) {
                        logger.info("### receive data  "+data);
                    }
//                    msg.clear();
                    msg=null;
                    source=null;

                    /**
                     * json反序列优化
                     * Gson实例在调用Json操作时不保持任何状态。
                     * 所以，可以自由地重复使用同一个对象来进行多个Json序列化和反序列化操作。
                     */
                    PgWal2JsonRecord record = gson.fromJson(data, PgWal2JsonRecord.class);
                    if (debug) {
                        logger.info("### receive record  "+record);
                    }
                    data=null;

                    if(record!=null&&record.getChange()!=null)
                    {
                        for (PgWalChange change : record.getChange()) {
                            String schema = change.getSchema();
                            String tableName = change.getTable();
                            if (debug) {
                                logger.info(schema+"."+tableName+" change debug #### ");
                            }

                            //调式使用
//                            if (schema.contains("ods_")) {
//                            }
//                            else
//                            {
                            if (
                                    checkSchemaList.contains(schema) && (
                                    !tableName.endsWith(Constants.VERSION_TABLE_SUB)
                                      || !tableName.equals(Constants.DATA_OPERATION_LOG_TABLE_NAME)
                                    )

                             ) {
                                //neo4j同步,只处理已经配置的表
                                if(PgWalClient.this.enableTableSyn(tableName)
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
                                        PgWalClient.this.enableModelDataSyn(tableName)
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
//                                    logger.info(schema+"."+tableName+" ModelData ==> ");


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
//                        }

                        record.unLinkPgWalChange();
                        record=null;
                    }

                    LogSequenceNumber lsn = stream.getLastReceiveLSN();
                    lastReceiveLsn=lsn.asString();
                    stream.setAppliedLSN(lsn);
                    stream.setFlushedLSN(lsn);


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
                if(stream==null)
                {
                    logger.error("create wal stream  error!#######################",e);
                }
                else
                {
                    logger.error("wal stream  read pending error!############################",e);
                }

            }finally {
                pgWalRunning.set(false);
                streamStartFlag.set(false);

                if(streamContext!=null)
                {
                    streamContext.close();
                }

            }

        }

    }

//    public Map<Integer, IProcessPipeline> getNoe4jProcessPipelineMap() {
//        return noe4jProcessPipelineMap;
//    }

    public Boolean getPgWalRunning() {
        return pgWalRunning.get();
    }

    public void setEnvName(String envName) {
        this.envName = envName;
    }

    public void setRabbitTemplate(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }
}
