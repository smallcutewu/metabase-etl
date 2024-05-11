package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.etl.tl.neo4j.config.*;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepositoryContext;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.etl.tl.neo4j.util.DbOperateUtil;
import com.boulderai.metabase.etl.tl.neo4j.util.Neo4jSyncType;
import com.boulderai.metabase.etl.tl.neo4j.util.SpringApplicationContext;
import com.boulderai.metabase.lang.Constants;
import com.boulderai.metabase.lang.er.ErType;
import com.boulderai.metabase.lang.util.SleepUtil;
//import com.boulderai.metabase.sync.core.service.neo4j.Neo4jPoolManager;
import com.google.common.cache.*;
import com.google.gson.Gson;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;


/**
 * @ClassName: 同步过程中一些常用上下文
 * @Description: 敞亮定义类
 * @author  df.l
 * @date 2022年09月14日
 * @Copyright boulderaitech.com
 */

public class Neo4jSyncContext {

    private  final static Logger logger = LoggerFactory.getLogger(Neo4jSyncContext.class);
    /**
     * 需要整个表同步到neo4j表
     */
//    private   ConcurrentHashMap<String,Integer> tableObjectSyncMap =new ConcurrentHashMap<String,Integer>(64);
//    private   ConcurrentHashMap<String,Integer> tableRelationSyncMap =new ConcurrentHashMap<String,Integer>(64);
//    private final ConcurrentHashMap<String,List<Map<String,Object>>> tableRelationSyncDetailMap =new ConcurrentHashMap<String,List<Map<String,Object>>>(64);
    /**
     *     关系已经删除，则对应neo4j之前的数据也要接触关系
     */
//    private final List<String> disenableTableObjectSyncList= new ArrayList<String>();
//    private final List<Map<String, Object>> disenableTableRelationSyncList= new ArrayList<Map<String, Object>>();

    private final ConcurrentHashMap<String,String> tablePrimaryKeyMap =new ConcurrentHashMap<String, String>(64);
    private final LinkedBlockingQueue<String>   waitingFindKeyQueue=new LinkedBlockingQueue<String>(10);
    //   private final ExecutorService   executorService;
    private final  Map<String, TableToBeanConfig>  tableToBeanConfigMap=new HashMap<String, TableToBeanConfig>();
    private final  Map<String, List<TableToBeanConfig>>  tableToBeanRelationConfigMap=new HashMap<String, List<TableToBeanConfig>>();

    private final  ConcurrentHashMap<Long, LogicEntityConfig>  logicEntityIdConfigMap=new ConcurrentHashMap<Long, LogicEntityConfig>();
    private final  ConcurrentHashMap<String, LogicEntityConfig>  logicEntityConfigMap=new ConcurrentHashMap<String, LogicEntityConfig>();
    private final  ConcurrentHashMap<Long, String>  logicEntityId2NameConfigMap=new ConcurrentHashMap<Long, String>();

    private final ConcurrentHashMap<String,String> allTablePrimaryKeyMap =new ConcurrentHashMap<String, String>(64);

    private final  ConcurrentHashMap<Long, String>  attrId2NameConfigMap=new ConcurrentHashMap<Long, String>();

    private final List<Integer>  checkRelList =   Arrays.asList(ErType.TTR.getType(),ErType.MMR.getType(),ErType.OTHER.getType(),ErType.MBR.getType());

    private final     Gson gson = new Gson();

    private ReentrantLock  pkMapLock=new ReentrantLock();

    private  PgWalConfig  pgWalConfig;

    private final CopyOnWriteArrayList<String> whiteTableList = new CopyOnWriteArrayList<>();

    private Boolean extractEngineRunning = false;

    public Boolean getExtractEngineRunning() {
        return extractEngineRunning;
    }

    public void setExtractEngineRunning(Boolean extractEngineRunning) {
        this.extractEngineRunning = extractEngineRunning;
    }

    public CopyOnWriteArrayList<String> getWhiteTableList() {
        return whiteTableList;
    }

    private Map<String,Integer>  targetBussinessTableMap= new ConcurrentHashMap<String, Integer>();

    public Map<String, Integer> getTargetBussinessTableMap() {
        return targetBussinessTableMap;
    }

    public void setTargetBussinessTableMap(Map<String, Integer> targetBussinessTableMap) {
        this.targetBussinessTableMap = targetBussinessTableMap;
    }

    public void addWhiteTableList(List<String> tables) {
        this.whiteTableList.addAll(tables);
    }

    private final LoadingCache<String, Map<String,String>> allTablePkMapCache = CacheBuilder.newBuilder()
            .maximumSize(100).expireAfterAccess(DataSynConstant.ONE_HOUR_SECONDS, TimeUnit.SECONDS)
            .recordStats()
            .build(
                    new CacheLoader<String, Map<String,String>>() {
                        @Override
                        public Map<String,String> load(String key) throws Exception {
                            Map<String,String>  pkMap=new HashMap<String,String>();
                            List<Map<String, Object>> pkMapList = DbOperateUtil.queryMapList(DataSynConstant.TABLE_PRIMARY_KEY_COLUMN_SQL);
                            if(CollectionUtils.isNotEmpty(pkMapList))
                            {
                                for (Map<String, Object>  map: pkMapList) {

                                    String tableSchema= (String) map.get("table_schema");
                                    if(tableSchema.contains("deploy"))
                                    {
                                        continue;
                                    }
                                    String tableName= (String) map.get("table_name");
                                    String columnName= (String) map.get("column_name");
                                    String pkKey=tableSchema+"#"+tableName;
                                    pkMap.put(pkKey,columnName);
                                }
                            }
                            return  pkMap;
                        }
                    }
            );

    //平滑升级多字段关系标志，完全上线以后设置为true
    private Boolean  useNewRelation=false;

    public String   getPkColumnBySchemaTable( String tableSchema,String tableName)
    {
        String pkKey=""+tableSchema+"#"+tableName;
        pkMapLock.lock();
        try{
            Map<String,String>  pkMap=   allTablePkMapCache.get(DataSynConstant.TABLE_ALL_PK_COLUMN_KEY);
            if(MapUtils.isNotEmpty(pkMap))
            {
                return pkMap.get(pkKey);
            }
        } catch (Exception e) {
        } finally {
            pkMapLock.unlock();
        }
        return  null;
    }

    public void checkLogicEntityConfig(String tableName)
    {
        if (!logicEntityConfigMap.containsKey(tableName)) {
            List<LogicEntityConfig> logicEntityConfigList = DbOperateUtil.queryBeanList(DataSynConstant.DA_LOGIC_ENTITY_CONFIG_BY_TABLE_SQL,LogicEntityConfig.class,tableName);
            if(CollectionUtils.isNotEmpty(logicEntityConfigList))
            {
                LogicEntityConfig  logicEntityConfig=logicEntityConfigList.get(0);
                logicEntityConfigMap.put(logicEntityConfig.getTableName(),logicEntityConfig);
                logicEntityIdConfigMap.put(logicEntityConfig.getId(),logicEntityConfig);
                logicEntityId2NameConfigMap.put(logicEntityConfig.getId(),logicEntityConfig.getTableName());

            }
        }
        getTablePrimaryKey( tableName);
    }

    private String getTableNameById(Long entityId)
    {
        String tableName=null;
        if(logicEntityId2NameConfigMap.containsKey(entityId))
        {
            tableName=logicEntityId2NameConfigMap.get(entityId);
        }
        else
        {
            Map<String, Object>  tableMap= DbOperateUtil.queryMap(DataSynConstant.SQL_TABLE_NAME_BY_ID,entityId);
            if(MapUtils.isNotEmpty(tableMap))
            {
                tableName= (String) tableMap.get("tableName");
                logicEntityId2NameConfigMap.put(entityId, tableName);
            }
        }
        return tableName;
    }


    private String getAttrNameById(Long attrId)
    {
        String attName=null;
        if(attrId2NameConfigMap.containsKey(attrId))
        {
            attName=logicEntityId2NameConfigMap.get(attrId);
        }
        else
        {
            Map<String, Object>  tableMap= DbOperateUtil.queryMap(DataSynConstant.SQL_ATTR_NAME_BY_ID,attrId);
            if(MapUtils.isNotEmpty(tableMap))
            {
                attName= (String) tableMap.get("attName");
                logicEntityId2NameConfigMap.put(attrId, attName);
            }
        }
        return attName;
    }



    private final  LoadingCache<String, Map<String,List<LogRelationConfig>> > logRelationConfigCache = CacheBuilder.newBuilder()
            .maximumSize(10000).expireAfterAccess(3600, TimeUnit.SECONDS)
            // 移除监听器
            .removalListener(
                    new RemovalListener<String, Map<String,List<LogRelationConfig>> >() {
                        @Override
                        public void onRemoval(RemovalNotification<String, Map<String,List<LogRelationConfig>> > rn) {

                        }
                    })
            .recordStats()
            .build(
                    new CacheLoader<String,  Map<String,List<LogRelationConfig>> >() {
                        @Override
                        public  Map<String,List<LogRelationConfig>>   load(String key) throws Exception {
                            String[] tableNames=key.split("#");

                            Map<String,List<LogRelationConfig>>  relationMap=new HashMap<String,List<LogRelationConfig>>();

                            if(tableNames.length<2)
                            {
                                return relationMap;
                            }

                            List<LogRelationConfig>  relationList=new ArrayList<LogRelationConfig>();
                            String tableName=tableNames[0];
                            Integer erType= NumberUtils.toInt(tableNames[1]);
                            checkLogicEntityConfig( tableName);
                            Long tableId=logicEntityConfigMap.get(tableName).getId();
                            List<LogRelationConfig> dbRelationConfigList=null;
//                            if(WalRecordModelDataRelationHandler.ERTYPE_MUTIL.equals(erType))
//                            {
                            dbRelationConfigList = DbOperateUtil.queryBeanList(DataSynConstant.SQL_LOGIC_RELATION_MULTI_CONFIG,LogRelationConfig.class,tableId,tableId);
//                            }
//                            else if (WalRecordModelDataRelationHandler.ERTYPE_SIMPLE.equals(erType)){
//                                relationConfigList = DbOperateUtil.queryBeanList(DataSynConstant.SQL_LOGIC_RELATION_SIMPLE_CONFIG,LogRelationConfig.class,tableId,tableId);
//                            }


                            if(CollectionUtils.isNotEmpty(dbRelationConfigList))
                            {
                                for (LogRelationConfig dbRelationConfig : dbRelationConfigList) {
                                    if(!checkRelList.contains(dbRelationConfig.getType()))
                                    {
                                        continue;
                                    }

                                    if(useNewRelation)
                                    {
                                        String attrMapping=dbRelationConfig.getAttrMapping();
                                        if(StringUtils.isBlank(attrMapping))
                                        {
                                            continue;
                                        }

                                        LogicRelationNodeConfig[] relArray = new Gson().fromJson(attrMapping, LogicRelationNodeConfig[].class);
                                        List<LogicRelationNodeConfig> mappingList = Arrays.asList(relArray);
                                        if(CollectionUtils.isEmpty(mappingList))
                                        {
                                            continue;
                                        }

                                        for (LogicRelationNodeConfig  nodeConfig: mappingList) {
                                            if (StringUtils.isBlank(nodeConfig.getStartAttrId())
                                                    || StringUtils.isBlank(nodeConfig.getEndAttrId())  ) {
                                                continue;
                                            }

                                            LogRelationConfig newRelationConfig=new LogRelationConfig();
                                            BeanUtils.copyProperties(dbRelationConfig,newRelationConfig);
                                            Long      startId=dbRelationConfig.getStartId();
                                            newRelationConfig.setStartTableName( getTableNameById( startId));

                                            Long      endId=dbRelationConfig.getEndId();
                                            newRelationConfig.setEndTableName(getTableNameById( endId));

                                            Long      startAttrId=NumberUtils.toLong(nodeConfig.getStartAttrId(),0L) ;
                                            Long      endAttrId = NumberUtils.toLong( nodeConfig.getEndAttrId(),0L) ;


                                            newRelationConfig.setStartAttrName(nodeConfig.getStartAttrCode());
                                            newRelationConfig.setEndAttrName(nodeConfig.getEndAttrCode());

                                            relationList.add(newRelationConfig);
                                        }
                                    }
                                    else
                                    {
                                        Long      startId=dbRelationConfig.getStartId();
                                        dbRelationConfig.setStartTableName( getTableNameById( startId));
                                        Long      endId=dbRelationConfig.getEndId();
                                        dbRelationConfig.setEndTableName(getTableNameById( endId));
                                        Long      startAttrId=dbRelationConfig.getStartAttrId() ;
                                        Long      endAttrId = dbRelationConfig.getEndAttrId();
                                        dbRelationConfig.setStartAttrName(getAttrNameById( startAttrId));
                                        dbRelationConfig.setEndAttrName(getAttrNameById( endAttrId));
                                        relationList.add(dbRelationConfig);
                                    }
                                }


                                for(LogRelationConfig  relation: relationList)
                                {
                                    Long  startId=relation.getStartId();
                                    Long  endId=relation.getEndId();
                                    String mapKey=startId+"_"+endId;
                                    List<LogRelationConfig> list = relationMap.computeIfAbsent(mapKey, k -> new ArrayList<LogRelationConfig>());
                                    list.add(relation);
                                }

                            }
                            relationList.clear();
                            return relationMap;
                        }
                    }
            );



//    private final  LoadingCache<String,Map<String, Set<Map<String ,Object>>> > modelDataCache = CacheBuilder.newBuilder()
//            .maximumSize(10000).expireAfterWrite(600, TimeUnit.SECONDS)
//            // 移除监听器
//            .removalListener(
//                    new RemovalListener<String,Map<String, Set<Map<String ,Object>>>>() {
//                        @Override
//                        public void onRemoval(RemovalNotification<String,Map<String, Set<Map<String ,Object>>>> rn) {
//                            Map<String, Set<Map<String ,Object>>>  map= rn.getValue();
//                            if(MapUtils.isNotEmpty(map))
//                            {
////                                for (Map.Entry< String, Set< Map<String, Object>> > entry: map.entrySet()) {
////                                    Set< Map<String, Object>>  setValue=entry.getValue();
////                                    if(CollectionUtils.isNotEmpty(setValue))
////                                    {
////                                        for(Map<String, Object> innerMap : setValue)
////                                        {
////                                            innerMap.clear();
////                                        }
////                                        setValue.clear();
////                                    }
////                                }
//                                map.clear();
//                            }
//                        }
//                    })
//            .recordStats()
//            .build(
//                    new CacheLoader<String, Map<String, Set<Map<String ,Object>>> >() {
//                        @Override
//                        public Map<String, Set<Map<String ,Object>>>  load(String tableName) throws Exception {
//
//                            Neo4jDataRepository  defaultNeo4jDataRepository=Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository();
//                            String cql="match (m:Module)-[edge1]-(n:Module) where m.handle='"+tableName+"' return m,edge1,n ";
//                            List<Record> resultList=defaultNeo4jDataRepository.queryByCql(cql);
//                            Map<String, Set<Map<String ,Object>>> retMap= Neo4jRelationUtil.parseRelations(resultList);
////                            String mapJson=gson.toJson(retMap);
////                            System.out.println(mapJson);
//                            return retMap;
//                        }
//                    }
//            );


    public   Map<String,List<LogRelationConfig>>  getModelDataConfigRelationByTable(String tableName,Integer erType)
    {
        String key=tableName+"#"+erType;
        try {
            return   logRelationConfigCache.get(key);
        } catch (Exception e) {
        }
        return null;
    }


    public   Boolean removeModelDataConfigRelationByTable(String tableName,Integer erType)
    {
        String key=tableName+"#"+erType;
        try {
            logRelationConfigCache.invalidate(key);
            return true;
        } catch (Exception e) {
        }
        return false;
    }

//    public  Map<String, Set<Map<String ,Object>>> getModelDataRelationByTable(String tableName)
//    {
//        try {
//            return  modelDataCache.get(tableName);
//        } catch (Exception e) {
//        }
//
//        return null;
//    }

    public  void initData()
    {
//        initTableConstraint();
        pgWalConfig=    SpringApplicationContext.getBean(PgWalConfig.class);
        initTableBeanConfig();
        initModelDataConfig();
        if (pgWalConfig!=null ) {
            useNewRelation=pgWalConfig.getUseNewRelation();
        }

    }


    public TableToBeanConfig  getTableToBeanConfigByTableName(String tableName)
    {
        return   tableToBeanConfigMap.get(tableName);
    }

    public List<TableToBeanConfig>  getTableToBeanRelationConfigByTableName(String tableName)
    {
        return   tableToBeanRelationConfigMap.get(tableName);
    }

    public void initModelDataConfig()
    {
        List<LogicEntityConfig> logicEntityConfigList = DbOperateUtil.queryBeanList(DataSynConstant.DA_LOGIC_ENTITY_CONFIG_SQL,LogicEntityConfig.class);
        if(CollectionUtils.isNotEmpty(logicEntityConfigList))
        {
            logicEntityConfigList.stream().filter(logicEntityConfig -> StringUtils.isNotBlank(logicEntityConfig.getTableName())
                    &&       ! logicEntityConfig.getTableName().endsWith(Constants.VERSION_TABLE_SUB) ).forEach(config -> {
                updateLogicEntityConfig(config);
            });
        }
        List<Map<String, Object>>  pkeyList= DbOperateUtil.queryMapList(DataSynConstant.ALL_TABLE_PRIMARY_KEY_SQL);
        if(CollectionUtils.isNotEmpty(pkeyList))
        {
            pkeyList.stream().filter(pkeyInfo -> StringUtils.isNotBlank((String) pkeyInfo.get("table_name"))).forEach(config -> {
                String tableName=(String) config.get("table_name");
                if(!tableName.endsWith(Constants.VERSION_TABLE_SUB))
                {
                    String pkeyName=(String) config.get("column_name");
                    allTablePrimaryKeyMap.put(tableName,pkeyName);
                }

            });
        }
        logicEntityConfigMap.remove(Constants.DATA_OPERATION_LOG_TABLE_NAME)  ;
        tableToBeanConfigMap.remove(Constants.DATA_OPERATION_LOG_TABLE_NAME)  ;
        tableToBeanRelationConfigMap.remove(Constants.DATA_OPERATION_LOG_TABLE_NAME)  ;
    }

    public  String getTablePrimaryKey(String tableName)
    {
        String primaryKey= allTablePrimaryKeyMap.get(tableName);
        if(StringUtils.isBlank(primaryKey))
        {
            List<Map<String, Object>>  pkeyList= DbOperateUtil.queryMapList(DataSynConstant.SQL_QUERY_TABLE_PRIMARY_KEY_BY_TABLE_NAME,tableName);
            if(CollectionUtils.isNotEmpty(pkeyList))
            {
                Map<String, Object>  pkMap=pkeyList.get(0);
                String pkTableName= (String) pkMap.get("table_name");
                primaryKey= (String) pkMap.get("column_name");
                allTablePrimaryKeyMap.put(tableName,primaryKey);
                pkeyList.clear();
            }
        }
        return primaryKey;
    }

    public  Boolean hasLogicEntityConfig(String tableName)
    {
        return logicEntityConfigMap.containsKey(tableName);
    }

    public  String getLogicEntityTableName(Long entityId)
    {
        if(!logicEntityId2NameConfigMap.containsKey(entityId))
        {
            List<LogicEntityConfig> logicEntityConfigList = DbOperateUtil.queryBeanList(DataSynConstant.DA_LOGIC_ENTITY_CONFIG_BY_TABLE_ID_SQL,LogicEntityConfig.class,entityId);
            if(CollectionUtils.isNotEmpty(logicEntityConfigList))
            {
                LogicEntityConfig  logicEntityConfig=logicEntityConfigList.get(0);
                if(logicEntityConfig!=null)
                {
                    if(StringUtils.isNotEmpty(logicEntityConfig.getTableName()))
                    {
                        logicEntityConfigMap.put(logicEntityConfig.getTableName(),logicEntityConfig);
                        logicEntityIdConfigMap.put(logicEntityConfig.getId(),logicEntityConfig);
                        logicEntityId2NameConfigMap.put(logicEntityConfig.getId(),logicEntityConfig.getTableName());
                    }


                }

            }
        }
        return logicEntityId2NameConfigMap.get(entityId);
    }

    public void updateLogicEntityConfig(LogicEntityConfig  config)
    {
        logicEntityIdConfigMap.put(config.getId(),config);
        logicEntityConfigMap.put(config.getTableName(),config);
        logicEntityId2NameConfigMap.put(config.getId(),config.getTableName());
    }

    public LogicEntityConfig  getLogicEntityById(Long id)
    {
        return  logicEntityIdConfigMap.get(id);
    }

    private void printTableBeanConfig()
    {
        for (Map.Entry< String,TableToBeanConfig> entry: tableToBeanConfigMap.entrySet()) {
            String key=entry.getKey();
            TableToBeanConfig value=entry.getValue();
            logger.info("tableToBeanConfigMap key= "+key+" ### "+value.toString());
            logger.info("###########################################");
        }

        for (Map.Entry< String,List<TableToBeanConfig>> entry: tableToBeanRelationConfigMap.entrySet()) {
            String key=entry.getKey();
            List<TableToBeanConfig> values=entry.getValue();
            logger.info("tableToBeanConfigMap key="+key+" $$$$$  values :  ");
            for (TableToBeanConfig value : values) {
                logger.info("TableToBeanConfig : "+ value.toString());
            }
            logger.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
        }
    }

    public void reInitTableBeanConfig(Boolean debug)
    {
        tableToBeanConfigMap.clear();
        tableToBeanRelationConfigMap.clear();
        initTableBeanConfig();
        if(debug)
        {
            printTableBeanConfig();
        }

    }

    public void initTableBeanConfig()
    {
        String synTypeObject=Neo4jSyncType.OnjectSyn.toString();
        List<TableToBeanConfig> tableToBeanConfigList = DbOperateUtil.queryBeanList(DataSynConstant.DA_TABLE_TO_BEAN_CONFIG_SQL,TableToBeanConfig.class);
        if(CollectionUtils.isNotEmpty(tableToBeanConfigList))
        {
            tableToBeanConfigList.forEach(config -> {
                if(synTypeObject.equals( config.getSynType()))
                {
                    tableToBeanConfigMap.put(config.getTableName(),config);
                    List<ColumnToFieldConfig> columnToFieldConfigList = DbOperateUtil.queryBeanList(DataSynConstant.DA_COLUMN_TO_FIELD_CONFIG_SQL, ColumnToFieldConfig.class,config.getBeanName());
                    if(CollectionUtils.isNotEmpty(columnToFieldConfigList))
                    {
                        columnToFieldConfigList.forEach(config::addColumnToFieldConfig);
                        config.init();
                    }
                }
                else
                {
                    List<TableToBeanConfig> relationConfigList = tableToBeanRelationConfigMap.computeIfAbsent(config.getTableName(), k -> new ArrayList<TableToBeanConfig>());
                    relationConfigList.add(config);
                }


            });
        }
        printTableBeanConfig();
    }

    public void clearNeo4jData()
    {
        logger.info("正在删除图数据库唯一键约束......");

        Map<String,Object> valuesMap = new HashMap<String,Object>(6);

        List<String>  pkList=new ArrayList<String>();

        for(Map.Entry<String, TableToBeanConfig>   entry:  tableToBeanConfigMap.entrySet())
        {
            TableToBeanConfig  config=entry.getValue();
            ColumnToFieldConfig     primaryKeyFieldConfig=config.getPrimaryKeyFieldConfig();
            if (StringUtils.isNotBlank(config.getBeanName())&&primaryKeyFieldConfig != null) {
                valuesMap.clear();
                valuesMap.put("tableName",config.getBeanName());
                valuesMap.put("pk",primaryKeyFieldConfig.getBeanField());
                StringSubstitutor sub = new StringSubstitutor(valuesMap);
                String  resCql= sub.replace(DataSynConstant.DROP_CONSTRAINT_CQL);
                Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository().operNeo4jDataByCqlSingleNoEx(resCql,false);
                logger.info("neo4j执行: "+resCql);
            }
        }


        List<String>  constraintCqlList=getConstraintCreateDropCqlList(DataSynConstant.DROP_CONSTRAINT_CQL,  pkList);
        for (String resCql: constraintCqlList) {
            Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository().operNeo4jDataByCqlSingleNoEx(resCql,false);
            logger.info("neo4j执行: "+resCql);
            SleepUtil.sleepMillisecond(200);
        }
        constraintCqlList.clear();



        logger.info("正在删除图数据库普通索引......");

        List<String>  normalIndexCqlList=getNormalIndexCreateDropCqlList(DataSynConstant.DROP_INDEX_NORMAL_CQL,  pkList);
        for (String resCql: normalIndexCqlList) {
            Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository().operNeo4jDataByCqlSingleNoExLog(resCql);
            logger.info("neo4j执行: "+resCql);
            SleepUtil.sleepMillisecond(200);
        }
        Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository().operNeo4jDataByCqlSingleNoExLog(" drop INDEX ON :Field(module_id) ");
        normalIndexCqlList.clear();

        logger.info("正在删除图数据表结构和数据......");
        pkList.clear();
        List<String>  tableDataCqlList=getConstraintCreateDropCqlList(DataSynConstant.DROP_TABLE_DATA_CONSTRUCTOR_CQL,  pkList);
        for (String resCql: tableDataCqlList) {
            Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository().operNeo4jDataByCqlSingleNoExLog(resCql);
            logger.info("neo4j执行: "+resCql);
            SleepUtil.sleepMillisecond(300);
        }
        tableDataCqlList.clear();

        pkList.clear();
        valuesMap.clear();
        logger.info("删除neo4j图数据库完成......");
    }

    private List<String>  getConstraintCreateDropCqlList(String cql,List<String>  pkList)
    {
        Map<String,Object> valuesMap = new HashMap<String,Object>(6);

        List<String>   checkSchemaList=Arrays.asList( Constants.CHECK_DATA_CHANGE_SCEMA.split(","));
        List<String>  cqlList=new ArrayList<String>();
        //读取表主键
        List<Map<String, Object>> tablePrimaryKeyQuery = DbOperateUtil.queryMapList(DataSynConstant.SQL_QUERY_ALL_TABLE_PRIMARY_KEY);
        if(CollectionUtils.isNotEmpty(tablePrimaryKeyQuery))
        {
            for (Map<String, Object> tablePrimaryMap : tablePrimaryKeyQuery) {
                String tableName= (String) tablePrimaryMap.get("table_name");
                String tableSchema= (String) tablePrimaryMap.get("table_schema");
                String pk= (String) tablePrimaryMap.get("column_name");
//                if(tableName.equals("bd_brand_classification_type"))
//                {
//                   System.out.println("");
//                }
                valuesMap.put("tableName",tableName);
                valuesMap.put("pk",pk);
                pkList.add(tableName+"#"+pk);
                StringSubstitutor sub = new StringSubstitutor(valuesMap);
                if (tableName.endsWith(Constants.VERSION_TABLE_SUB)
                        || !checkSchemaList.contains(tableSchema)) {
//                    String  resCql= sub.replace(dropUniqCql);
//                    Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository().operNeo4jDataByCqlSingleNoEx(resCql);
                }
                else {
                    String  resCql= sub.replace(cql);
//                    Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository().operNeo4jDataByCqlSingleNoEx(resCql);
                    cqlList.add(resCql);
                }

            }
            tablePrimaryKeyQuery.clear();
        }
        return  cqlList;

    }

    private List<String>  getNormalIndexCreateDropCqlList(String cql,List<String>  pkList)
    {
        List<String>  cqlList=new ArrayList<String>();
        Map<String,Object> valuesMap = new HashMap<String,Object>(6);
        List<Map<String, Object>> tableRelationQuery = DbOperateUtil.queryMapList(DataSynConstant.LOGIC_RELATION_TABLE_INFO_SQL);
        if(CollectionUtils.isNotEmpty(tableRelationQuery)) {
            for (Map<String, Object> tablePrimaryMap : tableRelationQuery) {
                String startTableName = (String) tablePrimaryMap.get("start_table_name");
                String endTableName = (String) tablePrimaryMap.get("end_table_name");
                String attrMapping = (String) tablePrimaryMap.get("attr_mapping");
                if (StringUtils.isEmpty(attrMapping)) {
                    continue;
                }
                LogicRelationNodeConfig[] relArray = new Gson().fromJson(attrMapping, LogicRelationNodeConfig[].class);
                if (relArray==null||relArray.length == 0) {
                    continue;
                }
                List<LogicRelationNodeConfig> mappingList = Arrays.asList(relArray);
                if(CollectionUtils.isEmpty(mappingList))
                {
                    continue;
                }

                if (
                        ( startTableName!=null&&  startTableName.endsWith(Constants.VERSION_TABLE_SUB)  )
                                ||  (endTableName!=null&& endTableName.endsWith(Constants.VERSION_TABLE_SUB)) ) {
                    continue;
                }


                if(mappingList.size()==1){
                    for (LogicRelationNodeConfig  logicRel : mappingList) {
                        String   startAttrCode=logicRel.getStartAttrCode();
                        String   endAttrCode=logicRel.getEndAttrCode();
                        if (StringUtils.isEmpty(startAttrCode)
                                || StringUtils.isEmpty(endAttrCode)  ) {
                            continue;
                        }
                        String startIndexKey=startTableName+"#"+startAttrCode;
                        String endIndexKey=endTableName+"#"+endAttrCode;

                        if(!pkList.contains(startIndexKey))
                        {
                            pkList.add(startIndexKey);
                            valuesMap.clear();

                            if(StringUtils.isEmpty(startTableName)
                                    || StringUtils.isEmpty(startAttrCode))
                            {
                                continue;
                            }
                            valuesMap.put("tableName",startTableName);
                            valuesMap.put("columnName",startAttrCode);
                            String  pkName=this.allTablePrimaryKeyMap.get(startTableName);

                            if(pkName!=null&&startAttrCode.equals(pkName))
                            {
                                continue;
                            }

                            StringSubstitutor sub = new StringSubstitutor(valuesMap);
                            String  resCql= sub.replace(cql);
                            cqlList.add(resCql);
                        }

                        if(!pkList.contains(endIndexKey))
                        {
                            pkList.add(endIndexKey);
                            if(StringUtils.isEmpty(endTableName)
                                    || StringUtils.isEmpty(endAttrCode))
                            {
                                continue;
                            }
                            valuesMap.clear();
                            valuesMap.put("tableName",endTableName);
                            valuesMap.put("columnName",endAttrCode);
                            String pkName= this.allTablePrimaryKeyMap.get(endTableName);
                            if(pkName!=null&&endAttrCode.equals(pkName))
                            {
                                continue;
                            }
                            StringSubstitutor sub = new StringSubstitutor(valuesMap);
                            String  resCql= sub.replace(cql);
                            cqlList.add(resCql);
//                        Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository().operNeo4jDataByCqlSingleNoExLog(resCql);
                        }
                    }
                }
                else if (mappingList.size() >1) {
                    valuesMap.clear();
                    StringBuilder  startAttBuilder=new StringBuilder(" ");
                    StringBuilder  endAttBuilder=new StringBuilder(" ");
                    for (LogicRelationNodeConfig  logicRel : mappingList) {
                        String startAttrCode = logicRel.getStartAttrCode();
                        startAttBuilder.append(startAttrCode).append(",");
                        String endAttrCode = logicRel.getEndAttrCode();
                        endAttBuilder.append(endAttrCode).append(",");
                    }
                    startAttBuilder.deleteCharAt(startAttBuilder.length() - 1);

                    valuesMap.put("tableName",startTableName);
                    valuesMap.put("columnName",startAttBuilder.toString());

                    StringSubstitutor sub = new StringSubstitutor(valuesMap);
                    String  startIndexCql= sub.replace(cql);
                    cqlList.add(startIndexCql);

                    valuesMap.clear();

                    endAttBuilder.deleteCharAt(endAttBuilder.length() - 1);
                    valuesMap.put("columnName",endAttBuilder.toString());
                    valuesMap.put("tableName",endTableName);

                    sub = new StringSubstitutor(valuesMap);
                    String  endIndexCql= sub.replace(cql);
                    cqlList.add(endIndexCql);
                }

            }
            tableRelationQuery.clear();
        }
        valuesMap.clear();
        return   cqlList;
    }

    /**
     * 初始化表主键信息
     */
    public void initTableConstraint()
    {
        logger.info("正在初始化图数据库唯一键约束......");
        if (this.pgWalConfig!=null&&StringUtils.isNotBlank(pgWalConfig.getSpecIndexList())) {
            String[]  specIndexs=pgWalConfig.getSpecIndexList().split(",");
            if (specIndexs.length > 0) {
                for (String specIndex : specIndexs) {
                    String resCql=" drop index "+specIndex+"  if  exists  ";
                    Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository()
                            .operNeo4jDataByCqlSingleNoEx(resCql,false);
                }
            }
        }

        Map<String,Object> valuesMap = new HashMap<String,Object>(6);
        List<String>  pkList=new ArrayList<String>();

        for(Map.Entry<String, TableToBeanConfig>   entry:  tableToBeanConfigMap.entrySet())
        {
            TableToBeanConfig  config=entry.getValue();
            ColumnToFieldConfig     primaryKeyFieldConfig=config.getPrimaryKeyFieldConfig();
            if (StringUtils.isNotBlank(config.getBeanName())&&primaryKeyFieldConfig != null) {
                valuesMap.clear();
                valuesMap.put("tableName",config.getBeanName());
                valuesMap.put("pk",primaryKeyFieldConfig.getBeanField());
                StringSubstitutor sub = new StringSubstitutor(valuesMap);
                String  resCql= sub.replace(DataSynConstant.CREATE_CONSTRAINT_CQL);
                Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository()
                        .operNeo4jDataByCqlSingleNoEx(resCql,false);
                logger.info("neo4j执行: "+resCql);
            }
        }




        List<String>  constraintCqlList=getConstraintCreateDropCqlList(DataSynConstant.CREATE_CONSTRAINT_CQL,  pkList);
        for (String resCql: constraintCqlList) {
            Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository().operNeo4jDataByCqlSingleNoEx(resCql,false);
            logger.info("neo4j执行: "+resCql);
            SleepUtil.sleepMillisecond(30);
        }
        constraintCqlList.clear();

        logger.info("正在初始化图数据库普通索引......");

        List<String>  normalIndexCqlList=getNormalIndexCreateDropCqlList(DataSynConstant.CREATE_INDEX_NORMAL_CQL,  pkList);
        for (String resCql: normalIndexCqlList) {
            Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository().operNeo4jDataByCqlSingleNoExLog(resCql);
            logger.info("neo4j执行: "+resCql);
            SleepUtil.sleepMillisecond(30);
        }
        Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository().operNeo4jDataByCqlSingleNoExLog(" CREATE INDEX ON :Field(module_id) ");
        normalIndexCqlList.clear();

        pkList.clear();
        valuesMap.clear();
    }






//    public List<String>  getTablePrimaryKeys(String tableName)
//    {
//        List<String>  primaryKeyList=tablePrimaryKeyMap.get(tableName);
//        if(CollectionUtils.isEmpty(primaryKeyList))
//        {
//            waitingFindKeyQueue.add(tableName);
////            executorService.submit(new CheckTablePrimeryKeyRunnable());
//        }
//        return primaryKeyList;
//    }



    /**
     * 初始化同步配置信息
     */
//    private void initSynConfig()
//    {
//        //刷新时候防止刷新过程中wal并发过滤table丢失,不能直接clear put
//        ConcurrentHashMap<String,Integer> tableObjectSyncTempMap =new ConcurrentHashMap<String,Integer>(64);
//        ConcurrentHashMap<String,Integer> tableRelationSyncTempMap =new ConcurrentHashMap<String,Integer>(64);
//
//        disenableTableObjectSyncList.clear();
//        disenableTableRelationSyncList.clear();
//
//        List<Map<String, Object>> synTableConfigList = DbOperateUtil.queryMapList(DataSynConstant.WAL_FILTER_TABLE_QUAERY_SQL);
//        if(!CollectionUtils.isEmpty(synTableConfigList))
//        {
//            for (Map<String, Object> configMap : synTableConfigList) {
//                String tableName = (String) configMap.get( "table_name");
//                String objectType = (String) configMap.get( "object_type");
//                Boolean  enable=  (Boolean) configMap.get( "enable");
//                if(enable)
//                {
//                    //如果是记录同步
//                    if(Neo4jSyncType.OnjectSyn.toString().equals(objectType))
//                    {
//                        if(!tableObjectSyncTempMap.containsKey(tableName))
//                        {
//                            tableObjectSyncTempMap.put(tableName,1);
//                        }
//                    }
//
//                    //如果是关系同步
//                    else  if(Neo4jSyncType.RelationSyn.toString().equals(objectType))
//                    {
//                        if(!tableRelationSyncTempMap.containsKey(tableName))
//                        {
//                            tableRelationSyncTempMap.put(tableName,1);
//                        }
//
//                        List<Map<String, Object>> syncColumnList = tableRelationSyncDetailMap.computeIfAbsent(tableName, k -> new ArrayList<Map<String, Object>>());
//                        syncColumnList.add(configMap);
//
//                    }
//                }
//                else {
//                    //如果之前配置需要处理对象同步而现在删除
//                    if(Neo4jSyncType.OnjectSyn.toString().equals(objectType))
//                    {
//                        disenableTableObjectSyncList.add(tableName);
//                    }
//                    //如果之前配置需要处理关系同步而现在删除
//                    else  if(Neo4jSyncType.RelationSyn.toString().equals(objectType))
//                    {
//                        disenableTableRelationSyncList.add(configMap);
//                    }
//                }
//
//            }
//            //算好替换
//            tableObjectSyncMap =tableObjectSyncTempMap;
//            tableRelationSyncMap=tableRelationSyncTempMap;
//        }
//    }

    /**
     * 此表是否需要同步，包括模型和关系
     * @param tableName  表名
     * @return  true需要同步，false不需要
     */
//    public Boolean isTableEnableSyn(String tableName)
//    {
//        return tableObjectSyncMap.containsKey(tableName) || tableRelationSyncMap.containsKey(tableName);
//    }

    public Boolean isTableEnableSynNew(String tableName)
    {
        return tableToBeanConfigMap.containsKey(tableName) || tableToBeanRelationConfigMap.containsKey(tableName);
    }

    /**
     * 是否同步记录
     * @param tableName  表名
     * @return 同步则true
     */
//    public Boolean isTableRecordEnableSyn(String tableName)
//    {
//        return tableObjectSyncMap.containsKey(tableName);
//    }

//    public Boolean isTableRelationEnableSyn(String tableName)
//    {
//        return tableRelationSyncMap.containsKey(tableName);
//    }

//    public  List<String> getTablePrimaryKeyList(String tableName) {
//        return tablePrimaryKeyMap.get(tableName);
//    }

    /**
     * 获取一张表的同步列字段列表
     * @param tableName  表名称
     * @return  所有同步列信息
     */
//    public  List<Map<String, Object>> getEnableTableRelationSyncList(String tableName) {
//        List<Map<String,Object>> list=tableRelationSyncDetailMap.get(tableName);
//        return Collections.unmodifiableList(list);
//    }

    /**
     * 获取已经被关闭的表对象同步列表
     * @return  关闭的同步列表
     */
//    public  List<String> getDisenableTableObjectSyncList() {
//        return Collections.unmodifiableList(disenableTableObjectSyncList) ;
//    }
//
//
//    /**
//     * 获取已经关闭的表关系同步列表
//     * @return  关系同步列表
//     */
//    public  List<Map<String, Object>> getDisenableTableRelationSyncList() {
//        return Collections.unmodifiableList(disenableTableRelationSyncList);
//    }

    /**
     * 单例内部类
     */
    private static class Holder{
        private static final Neo4jSyncContext INSTANCE = new Neo4jSyncContext();
    }

    public static  Neo4jSyncContext  getInstance() {
        return Holder.INSTANCE;
    }
}
