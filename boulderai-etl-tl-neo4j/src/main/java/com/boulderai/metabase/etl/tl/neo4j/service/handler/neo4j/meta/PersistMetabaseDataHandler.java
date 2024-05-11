package com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.meta;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.*;
import com.boulderai.metabase.lang.Constants;
import com.boulderai.metabase.etl.tl.neo4j.config.ColumnToFieldConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.LogicEntityConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.TableToBeanConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.BaseDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.IDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.INeo4jDataRepository;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepositoryContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.Neo4jSyncContext;
import com.boulderai.metabase.etl.tl.neo4j.util.DbOperateUtil;
import com.boulderai.metabase.etl.tl.neo4j.util.Neo4jSyncType;
import com.boulderai.metabase.etl.tl.neo4j.util.SpringApplicationContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.neo4j.driver.Values.parameters;


/**
 * @ClassName: PersistMetabaseDataHandler
 * @Description: 配置节点数据持久化处理器
 * @author  df.l
 * @date 2022年10月12日
 * @Copyright boulderaitech.com
 */
@Slf4j
public class PersistMetabaseDataHandler extends BaseDataHandler {


//MATCH (n:Module) where n.handle =~ '.*_ver__backup' detach delete n
//match (n:Field)  where n.name =~'.*_ver__backup.*' detach delete n

    private static  final String  DATA_AVAILABILITY_COLUMN="data_availability";
    private static  final String  PRIMARY_KEY_COLUMN="primary_key";

    private static final List<String>  checkTableModeTables=new ArrayList<String>();
    private static final   Neo4jSyncContext  neo4jSyncContext =Neo4jSyncContext.getInstance();
    static {
        checkTableModeTables.add("da_logic_entity");
    }

    private static PgWalConfig pgWalConfig=null;

    /**
     * table+oper, cql
     */
    private final Map<String,String> neo4jOperCqlMap=new HashMap<String,String>(10);

    private final Map<String, CqlValueContainer> cqlValueListMap=new HashMap<String,CqlValueContainer>(10);

    private void checkModelTableBuffer(String tableName)
    {
        if(checkTableModeTables.contains(tableName))
        {
            neo4jSyncContext.checkLogicEntityConfig(tableName);
        }
    }



    @Override
    protected void processDetail(List<PgWalChangeAck> dataList, List<String>  cqlList) {

        /**
         * 清空上一次组装数据行
         */
        if(!cqlValueListMap.isEmpty())
        {
            for(CqlValueContainer  container : cqlValueListMap.values())
            {
                container.clear();
            }
            cqlValueListMap.clear();
        }

        if(pgWalConfig==null)
        {
            pgWalConfig= SpringApplicationContext.getBean(PgWalConfig.class);
        }

        /**
         * 批量构造本次修改行
         */
        for (PgWalChangeAck changeAck : dataList) {
            PgWalChange walChange=changeAck.getChange();
            String  tableName=walChange.getTable();
            //如果只是同步与本表相关的其他表关系不同步本记录
            TableToBeanConfig tableToBeanConfig=neo4jSyncContext.getTableToBeanConfigByTableName(tableName);
            if(tableToBeanConfig==null||
                   ! tableToBeanConfig.getSynType().equals(Neo4jSyncType.OnjectSyn.toString()))
            {
                continue;
            }

            String checkColumn= tableToBeanConfig.getCheckColumn();
            String checkValue= tableToBeanConfig.getCheckValue();

            //目前只处理da_logic_entity  status=2,通过配置化以后增加不需要硬编码
            if(StringUtils.isNotBlank(checkColumn)||
                    StringUtils.isNotBlank(checkValue))
            {
                Column column =  walChange.makeOneColumn(checkColumn);
                if(column==null  )
                {
                    continue;
                }
                Object  columnValue=column.getRealValue();
                //da_logic_entity  status!=2
//                if(!checkValue.equals(String.valueOf(columnValue)))
//                {
//                    continue;
//                }
                //多个值，有个字段有多值了现在
                if(! tableToBeanConfig.hasCheckValue(String.valueOf(columnValue))  )
                {
                    if (tableName.equals(DA_LOGIC_ENTITY_TABLE)) {
                        log.info("逻辑实体尚未发布! "+ walChange);
                    }
                    column.clear();
                    continue;
                }
            }

            if (tableName.equals(DA_ENTITY_ATTRIBUTE_TABLE))
            {
                Column column=  walChange.makeOneColumn(ENTITY_ID_COLUMN);
                if(column!=null&&column.getRealValue()!=null)
                {
                    Long entityId= (Long) column.getRealValue();
                   String realTableName=  neo4jSyncContext.getLogicEntityTableName(entityId);
                   if(realTableName!=null&&realTableName.endsWith(Constants.VERSION_TABLE_SUB))
                   {
                       deleteObject( entityId);
                       continue;
                   }
                }
            }

            if (tableName.equals(DA_LOGIC_ENTITY_TABLE))
            {
                Column column=  walChange.makeOneColumn(TABLE_NAME_COLUMN);
                if(column!=null&&column.getRealValue()!=null)
                {
                    String realTableName= (String) column.getRealValue();
                    if(realTableName!=null&&realTableName.endsWith(Constants.VERSION_TABLE_SUB))
                    {
                        column=  walChange.makeOneColumn(LOG_ENTITY_ID_COLUMN);
                        if (column != null && column.getRealValue() != null) {
                            Long entityId= (Long) column.getRealValue();
                            deleteObject( entityId);
                            continue;
                        }

                    }
                }
            }

            AbstractRowEvent  row= walChange.getChangeToRow();
            EventType eventType=row.getEventType();
            String  eventTypeStr=eventType.toString();
            String cqlKey=tableName+"#"+eventTypeStr;
            String  cql=neo4jOperCqlMap.get(cqlKey);

//            List<String> primaryKeys=Neo4jSyncContext.getInstance().getTablePrimaryKeys(tableName);

            /**
             * 如果之前cql没有生成过，则去构造
             */
            if(cql==null)
            {
                //insert事件
                if(EventType.INSERT.toString().equals(eventTypeStr))
                {
                    cql=   makeNewInsertCql(  tableName, cqlKey,  row,walChange);
                    checkModelTableBuffer( tableName);
                }
                //update事件
                else     if(EventType.UPDATE.toString().equals(eventTypeStr))
                {
                    if (isDeleteRecord( walChange)) {
                        cql=   makeDeleteCql(  tableName, cqlKey,  row);
                    }
                    else
                    {
                        cql=   makeUpdateCql(  tableName, cqlKey,  row);
                        checkModelTableBuffer( tableName);
                    }

                }

                //delete事件
                else   if(EventType.DELETE.toString().equals(eventTypeStr))
                {
                    cql=   makeDeleteCql(  tableName, cqlKey,  row);
                }

            }

            //cql转md5
            String   cqlValueId=CqlValueContainer.cql2Id(cql);
            String tableOperKey=tableName+"_"+eventTypeStr+"_"+cqlValueId;

            //同一张表同一种操作类型放在一起批量提交，没有则创建
            CqlValueContainer cqlValueContainer=   cqlValueListMap.get(tableOperKey);
            if(cqlValueContainer==null)
            {
                cqlValueContainer=new CqlValueContainer(cql,  eventType,  tableName);
                cqlValueListMap.put(tableOperKey,cqlValueContainer);
            }

            ColumnToFieldConfig     primaryKeyFieldConfig=tableToBeanConfig.getPrimaryKeyFieldConfig();
            List<ColumnToFieldConfig> notKeyColumnToFieldConfigList =tableToBeanConfig.getNotKeyColumnToFieldConfigList();
            //delete事件值组装
            if(EventType.DELETE.toString().equals(eventTypeStr))
            {
                List<Column> primaryKeyColumns=row.getPrimaryKeyColumns();
                Object  columnValue=getColumnValueByName(primaryKeyColumns, primaryKeyFieldConfig.getTableColumn(),primaryKeyFieldConfig);
                Object[] voArray={primaryKeyFieldConfig.getBeanField(),columnValue};
                Value  value=parameters( voArray);
                cqlValueContainer.addValue(value);
            }
            else  if(EventType.UPDATE.toString().equals(eventTypeStr)  ) {
                if (isDeleteRecord( walChange)) {
                    List<Column> primaryKeyColumns=row.getPrimaryKeyColumns();
                    Object  columnValue=getColumnValueByName(primaryKeyColumns, primaryKeyFieldConfig.getTableColumn(),primaryKeyFieldConfig);
                    Object[] voArray={primaryKeyFieldConfig.getBeanField(),columnValue};
                    Value  value=parameters( voArray);
                    cqlValueContainer.addValue(value);
                }
                else
                {
                    List<Object> columnValueList = new ArrayList<Object>();
                    //如果是更新事件则需要增加主键列

                    if (primaryKeyFieldConfig != null) {
                        setConfig2ColumnValue(primaryKeyFieldConfig , walChange,    columnValueList);
                    }

                    //增加需要更新的列值
                    for (ColumnToFieldConfig config : notKeyColumnToFieldConfigList) {
                        if (DA_LOGIC_ENTITY_TABLE.equals(tableName)
                            && config.getBeanField().equals(PRIMARY_KEY_COLUMN)) {
                            continue;
                        }
                       else  if(config.getBeanField().equals(DATA_AVAILABILITY_COLUMN))
                        {
                            if(!pgWalConfig.getFieldDataAvailabilityCanUpdate())
                            {
                                continue;
                            }
                        }
                        setConfig2ColumnValue(config , walChange,    columnValueList);
                    }

                    if (DA_ENTITY_ATTRIBUTE_TABLE.equals(tableName)) {
                        checkEntityAttribute( walChange,   columnValueList);
                    }
                   else if (DA_LOGIC_ENTITY_TABLE.equals(tableName)) {
                        checkTablePrimaryKey( walChange,   columnValueList);
                    }

                    //将值列按序封装成对象
                    Object[] voArray = columnValueList.toArray(new Object[columnValueList.size()]);
                    Value value = parameters(voArray);
                    cqlValueContainer.addValue(value);
                    columnValueList.clear();
                }


            }


            else  if( EventType.INSERT.toString().equals(eventTypeStr)   )
            {
                List<Object>   columnValueList=new ArrayList<Object>();
                //MERGE   (p:Person{name:"zhangsan"})
                if (primaryKeyFieldConfig!=null) {
                    setConfig2ColumnValue(primaryKeyFieldConfig , walChange,    columnValueList);
                }

                //on CREATE
                for (ColumnToFieldConfig config : notKeyColumnToFieldConfigList) {
                    setConfig2ColumnValue(config , walChange,    columnValueList);
                }
                if (DA_ENTITY_ATTRIBUTE_TABLE.equals(tableName)) {
                    checkEntityAttribute( walChange,   columnValueList);
                }

                //on  MATCH  不要调动位置，这里代码和cql一一对应
                for (ColumnToFieldConfig config : notKeyColumnToFieldConfigList) {
                    setConfig2ColumnValue(config , walChange,    columnValueList);
                }
                if (DA_ENTITY_ATTRIBUTE_TABLE.equals(tableName)) {
                    checkEntityAttribute( walChange,   columnValueList);
                    checkEntityDataAvailability( walChange,   columnValueList);
                }
                else if (DA_LOGIC_ENTITY_TABLE.equals(tableName)) {
                    checkTablePrimaryKey( walChange,   columnValueList);
                }

                //将值列按序封装成对象
                Object[] voArray=  columnValueList.toArray(new Object[columnValueList.size()]);
                Value  value=parameters( voArray);
                cqlValueContainer.addValue(value);
                columnValueList.clear();
            }

        }

        if(!cqlValueListMap.isEmpty() && neo4jSyncContext.getExtractEngineRunning())
        {
            INeo4jDataRepository dataRepository = Neo4jDataRepositoryContext.get(this.getPipelineId());
            for(CqlValueContainer  container : cqlValueListMap.values())
            {
                dataRepository.operNeo4jDataByCqlSingle(container);
            }
        }

    }

    private void checkTablePrimaryKey(PgWalChange walChange,List<Object>   columnValueList)
    {
        String tableName="";
        String tableSchema="";
        Column tableNameColumn =  walChange.makeOneColumn("table_name");
        if(tableNameColumn!=null)
        {
            tableName= (String) tableNameColumn.getRealValue();
        }
        Column namespaceCodeColumn =  walChange.makeOneColumn("namespace_code");
        if(namespaceCodeColumn!=null)
        {
            tableSchema= (String) namespaceCodeColumn.getRealValue();
        }
        columnValueList.add("primary_key");
        String  primaryKey=Neo4jSyncContext.getInstance().getPkColumnBySchemaTable(  tableSchema, tableName);
        if(StringUtils.isBlank(primaryKey))
        {
            primaryKey="";
        }
        columnValueList.add(primaryKey);
    }

    private void checkEntityDataAvailability(PgWalChange walChange,List<Object>   columnValueList)
    {
        columnValueList.add("data_availability");
        columnValueList.add(0.0f);
    }

    private void checkEntityAttribute(PgWalChange walChange,List<Object>   columnValueList)
    {
        columnValueList.add("handle");
        Column column =  walChange.makeOneColumn("entity_id");
        String handle="";
        String tableName = "";
        LogicEntityConfig  config=null;
        Long entityId=0L;
        if (column != null) {
             entityId= (Long) column.getRealValue();
            config= neo4jSyncContext.getLogicEntityById(entityId);
        }
        if (config != null) {
            handle=config.getCode();
            tableName=config.getTableName();
        }

        if(StringUtils.isBlank(handle))
        {
            handle="";
        }
        columnValueList.add(handle);

        columnValueList.add("table_name");
        if(StringUtils.isBlank(tableName))
        {
            tableName="";
        }
        columnValueList.add(tableName);
    }

    private void setConfig2ColumnValue(ColumnToFieldConfig config ,PgWalChange walChange, List<Object>   columnValueList)
    {
        String  columnName=config.getTableColumn();
        columnValueList.add(config.getBeanField());
        if(StringUtils.isNotBlank(columnName))
        {
            Column column =  walChange.makeOneColumn(columnName);
            if(column!=null)
            {
                Object cValue=column.getRealValue();
                if (cValue != null) {
                    if(FIELD_TYPE_STRING.equals(config.getBeanFieldType()))
                    {
                        columnValueList.add(String.valueOf(column.getRealValue()));
                    }
                    else
                    {
                        columnValueList.add(column.getRealValue());
                    }
                }
                else {
                    columnValueList.add("");
                }


                column.clear();
            }
            else {
                String  extendColumnSql=config.getExtendColumnSql();
                String  extendSqlColumns=config.getExtendSqlColumns();
                String  extendSqlReturnField=config.getExtendSqlReturnField();
                if(StringUtils.isNotBlank(extendColumnSql))
                {
                    //，目前只支持一个字段参数
                    Object  pValue=null;
                    if(StringUtils.isNotBlank(extendSqlColumns))
                    {
                        Column extColumn =  walChange.makeOneColumn(extendSqlColumns);
                        if(extColumn!=null)
                        {
                            pValue=extColumn.getRealValue();
                        }
                    }
                    Map<String, Object> extColumnValueMap = DbOperateUtil.queryMap(extendColumnSql,pValue);
                    if(extColumnValueMap!=null)
                    {
                       Object returnValue= extColumnValueMap.getOrDefault(extendSqlReturnField,"");
                        columnValueList.add(String.valueOf(returnValue));
                    }
                    else
                    {
                        setColumnDefaultValue( config , walChange,    columnValueList);
                    }
                }
                else
                {
                    setColumnDefaultValue( config , walChange,    columnValueList);
                }
            }
        }
        else
        {
            setColumnDefaultValue( config , walChange,    columnValueList);

        }

    }

    private void setColumnDefaultValue(ColumnToFieldConfig config ,PgWalChange walChange, List<Object>   columnValueList)
    {
        if(FIELD_TYPE_STRING.equals(config.getBeanFieldType()))
        {
            columnValueList.add("");
        }
        else if(FIELD_TYPE_BOOLEAN.equals(config.getBeanFieldType()))
        {
            columnValueList.add(false);
        }
        else if(FIELD_TYPE_FLOAT.equals(config.getBeanFieldType()))
        {
            columnValueList.add(0f);
        }
        else
        {
            columnValueList.add("");
        }
    }

    private Column  getColumnByName(List<Column> columns,String tableColumn)
    {
        for (Column column : columns) {
            if(column.getName().equals(tableColumn))
            {
                return  column;
            }
        }
        return null;
    }

    private Object  getColumnValueByName(List<Column> columns,String tableColumn, ColumnToFieldConfig     config)
    {
        for (Column column : columns) {
            if(column.getName().equals(tableColumn))
            {
                Object  value=  column.getRealValue();
                if(value!=null)
                {
                    if(FIELD_TYPE_STRING.equals(config.getBeanFieldType()))
                    {
                        return String.valueOf(value);
                    }
                    else
                    {
                        return value;
                    }
                }

                column.clear();
                return  "";
            }
        }
        return null;
    }


    @Override
    public String name() {
        return IDataHandler.PERSIST_DATA_HANDLER_NAME;
    }

    @Override
    public void  close()
    {
        neo4jOperCqlMap.clear();
    }



    /**
     * 生成insert  cql语句
     * @param tableName  表名
     * @param cqlKey  cql键
     * @param row 数据变化事件
     * @return  cql
     */
    private   String makeNewInsertCql(String  tableName,String cqlKey,AbstractRowEvent  row,PgWalChange walChange)
    {
            TableToBeanConfig tableToBeanConfig=neo4jSyncContext.getTableToBeanConfigByTableName(tableName);
            String  beanName=tableToBeanConfig.getBeanName();

//            StringBuilderPool pool=StringBuilderPool.getInstance();
//            StringBuilder  cqlBuilder=pool.borrowObject();

        StringBuilder  cqlBuilder=new StringBuilder();
            //创建cql
            cqlBuilder.append(" MERGE ( t:").append(beanName).append(" { ");
//            InsertRowEvent   event=(InsertRowEvent)  row;
            //MERGE   (p:Person{name:"zhangsan"})
            ColumnToFieldConfig primaryKeyFieldConfig  = tableToBeanConfig.getPrimaryKeyFieldConfig();
//            int keySize=primaryKeys.size();
            if(primaryKeyFieldConfig!=null)
            {
                String  beanField=primaryKeyFieldConfig.getBeanField();
//                Column column =  walChange.makeOneColumn(columnName);
                cqlBuilder.append(beanField).append(":") ;
                cqlBuilder.append("$").append(beanField);
            }

            cqlBuilder.append(" })   " );
            cqlBuilder.append("  \n\t    on CREATE  set   ") ;

//            StringBuilder  cqlMergeBuilder=pool.borrowObject();
           StringBuilder  cqlMergeBuilder=new StringBuilder();
            cqlMergeBuilder.append("  \n\t on MATCH  set  ");
            //组装列
//            List<Column> columns=event.getColumns();

            List<ColumnToFieldConfig> columnToFieldConfigList = tableToBeanConfig.getNotKeyColumnToFieldConfigList();
            int size=columnToFieldConfigList.size();
            for(int i=0;i<size;i++)
            {
                ColumnToFieldConfig  config=columnToFieldConfigList.get(i);
                cqlBuilder.append(" t.").append(config.getBeanField()).append("=") .append("$").append(config.getBeanField());
                if(i<size-1)
                {
                    cqlBuilder.append(", ");
                }

                cqlMergeBuilder.append(" t.").append(config.getBeanField()).append(" = ") .append("   $").append(config.getBeanField());
                if(i<size-1)
                {
                    cqlMergeBuilder.append(", ");
                }
            }
//            if (DA_ENTITY_ATTRIBUTE_TABLE.equals(tableName)) {
//                cqlBuilder.append(", t.handle = ") .append("   $handle");
//            }
//            if (DA_ENTITY_ATTRIBUTE_TABLE.equals(tableName)) {
//                cqlMergeBuilder.append(", t.handle = ") .append("   $handle");
//            }

            String mergeStmt=cqlMergeBuilder.toString();
            cqlBuilder.append(mergeStmt);

            String statement=cqlBuilder.toString();
            //保存起来重用
            neo4jOperCqlMap.put(cqlKey,statement);
//            pool.returnObject(cqlBuilder);
//            pool.returnObject(cqlMergeBuilder);
        cqlBuilder=null;
        cqlMergeBuilder=null;
        return statement;

    }




    /**
     * 生成update  cql语句
     * @param tableName  表名
     * @param cqlKey  cql键
     * @param row 数据变化事件
     * @return  cql
     */
    private   String makeUpdateCql(String  tableName,String cqlKey,AbstractRowEvent  row)
    {

        TableToBeanConfig tableToBeanConfig=neo4jSyncContext.getTableToBeanConfigByTableName(tableName);
        String  beanName=tableToBeanConfig.getBeanName();

//
//        StringBuilderPool pool=StringBuilderPool.getInstance();
//        StringBuilder  cqlBuilder=pool.borrowObject();
        StringBuilder cqlBuilder =new StringBuilder();
        //创建update cql
        cqlBuilder.append(" MERGE   ( t:").append(beanName).append(" { ");
//        UpdateRowEvent   event=(UpdateRowEvent)  row;

        //组装主键列
//        List<Column> primaryKeyColumns=event.getPrimaryKeyColumns();
//        int size=primaryKeyColumns.size();
        ColumnToFieldConfig primaryKeyFieldConfig  = tableToBeanConfig.getPrimaryKeyFieldConfig();
       if(primaryKeyFieldConfig!=null)
        {
            cqlBuilder.append(primaryKeyFieldConfig.getBeanField()).append(" : $") .append(primaryKeyFieldConfig.getBeanField());
        }
        cqlBuilder.append(" } ");

        //合并列 MERGE (n:Page {id:1 })  SET n.url="a111bc" ,n.test=123
        cqlBuilder.append("  )   set ");
//        List<Column> columns=event.getColumns();

        List<ColumnToFieldConfig> columnToFieldConfigList = tableToBeanConfig.getNotKeyColumnToFieldConfigList();
        int size=columnToFieldConfigList.size();
        for(int i=0;i<size;i++)
        {
            ColumnToFieldConfig  config=columnToFieldConfigList.get(i);

            if(config.getBeanField().equals(DATA_AVAILABILITY_COLUMN))
            {
                if(!pgWalConfig.getFieldDataAvailabilityCanUpdate())
                {
                    continue;
                }
            }

            cqlBuilder.append("  t.");
            cqlBuilder.append(config.getBeanField()).append(" = ") .append("  $").append(config.getBeanField());
            if(i<size-1)
            {
                cqlBuilder.append(",");
            }
        }
//        if (DA_ENTITY_ATTRIBUTE_TABLE.equals(tableName)) {
//            cqlBuilder.append(", t.handle = ") .append("   $handle");
//        }
        String statement=cqlBuilder.toString();
        neo4jOperCqlMap.put(cqlKey,statement);
//        pool.returnObject(cqlBuilder);
        cqlBuilder=null;
        return statement;
    }


    /**
     * 生成delete  cql语句
     * @param tableName  表名
     * @param cqlKey  cql键
     * @param row 数据变化事件
     * @return  cql
     */
    private   String makeDeleteCql(String  tableName,String cqlKey,AbstractRowEvent  row)
    {
        TableToBeanConfig tableToBeanConfig=neo4jSyncContext.getTableToBeanConfigByTableName(tableName);
        String  beanName=tableToBeanConfig.getBeanName();

//        StringBuilderPool pool=StringBuilderPool.getInstance();
//        StringBuilder  cqlBuilder=pool.borrowObject();
        StringBuilder  cqlBuilder=new StringBuilder();
        //delete  cql
        cqlBuilder.append(" MATCH  ( t:").append(beanName).append(" { ");
        ColumnToFieldConfig primaryKeyConfig=tableToBeanConfig.getPrimaryKeyFieldConfig();

        if (primaryKeyConfig != null) {
            cqlBuilder.append(primaryKeyConfig.getBeanField()).append(":") .append(" $").append(primaryKeyConfig.getBeanField());
        }
        cqlBuilder.append(" }) ");
        cqlBuilder.append(" DETACH DELETE t ");
        String statement=cqlBuilder.toString();
        neo4jOperCqlMap.put(cqlKey,statement);
//        pool.returnObject(cqlBuilder);
        cqlBuilder=null;
        return statement;
    }

    public Map<String, String> getNeo4jOperCqlMap() {
        return neo4jOperCqlMap;
    }

    public Map<String, CqlValueContainer> getCqlValueListMap() {
        return cqlValueListMap;
    }
}
