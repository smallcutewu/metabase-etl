package com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.model;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.*;
import com.boulderai.metabase.lang.er.ErType;
import com.boulderai.metabase.etl.tl.neo4j.config.LogRelationConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.BaseDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.INeo4jDataRepository;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepository;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepositoryContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.Neo4jSyncContext;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.etl.tl.neo4j.util.DbOperateUtil;
import com.boulderai.metabase.etl.tl.neo4j.util.ExceptionUtils;
import com.boulderai.metabase.etl.tl.neo4j.util.SpringApplicationContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.*;

/**
 * @ClassName: WalRecordModelDataRelationHandler
 * @Description: 模型节点关系持久化处理器
 * @author  df.l
 * @date 2022年10月26日
 * @Copyright boulderaitech.com
 */
@Slf4j
public class WalRecordModelDataRelationHandler extends BaseDataHandler {
    //    private final static String RELATION_UPSERT_CQL=" match (left:${fromLabel} { ${fromField}:${toColumnValue}   } ) , ( right:${toLabel} { ${toColumn}:${toColumnValue}   })  merge (left) -[r:DER {re_id:'${reId}' } ] ->(right) set  r.from='${fromField}',r.to='${toField}', r.erType='${erType}', r.re_fields='${reFields}',r.re_ids='${reIds}',r.type='${type}',r.multi_er='${multiEr}'  ";
    private final static String RELATION_UPSERT_CQL=" match (left:${fromLabel} { ${fromField}:${toColumnValue}   } ) , ( right:${toLabel} { ${toColumn}:${toColumnValue}   })  merge (left) -[r:DER {re_id:'${reId}' } ] ->(right) set  r.from='${fromField}',r.to='${toField}', r.erType='${erType}', r.re_fields='${reFields}',r.re_ids='${reIds}',r.type='${type}',r.multi_er='${multiEr}',r.SelfR=${selfReferenced}  ";
    private final static String RELATION_DELETE_CQL=" match (t:${tableLabel} { ${primaryKey}:${primaryKeyValue} } )  detach delete t   ";
    //    private final static String NODE_MISS_CQL=" MERGE (t:${tableLabel} { ${primaryKey}:${primaryKeyValue} }  ) on create set t  = {  ${primaryKey}:${primaryKeyValue} ,desc:'临时节点，此表主键必须为对方外键' } ";
    private final static String RELATION_UPSERT_DELETE_CQL=" match (left:${fromLabel} { ${primaryKey}:${primaryKeyValue}   } )  - [ r:DER { re_id:'${reId}' }] -> ( right:${toLabel} { ${toColumn}:${toColumnValue}   }) delete r ";

    //    private final List<String>  relationCqlList=  new ArrayList<String>();
    private final Map<String,Object>  relationParamMap=  new HashMap<String,Object>(16);

    private final List<Integer>  checkRelList =   Arrays.asList(ErType.TTR.getType(),ErType.MMR.getType(),ErType.MBR.getType(),ErType.OTHER.getType());

    public final  static  Integer  ERTYPE_SIMPLE=1;

    public final  static  Integer  ERTYPE_MUTIL=2;

    public final static String DEFAUT_SHCHAME = "public";

    public  WalRecordModelDataRelationHandler()
    {
        this.relogError=false;
    }


    @Override
    public void  close()
    {
//        relationCqlList.clear();
    }

    private Map<Long,Map<String ,Object>>  parse(Set<Map<String ,Object>>   nodeList)
    {
        Map<Long,Map<String ,Object>>  nodeMap=new HashMap<Long, Map<String, Object>>();
        for (Map<String, Object> node: nodeList) {
            Long nodeId= (Long) node.get("nodeId");
            nodeMap.put(nodeId, node);
        }
        return nodeMap;
    }

    private void handleModelDataRelDetail(List<PgWalChangeAck> dataList,List<String>  cqlList)
    {
        //让模型先进去等待
//        if(CollectionUtils.isNotEmpty(dataList)&&dataList.size()>1)
//        {
//            SleepUtil.sleepMillisecond(1000);
//        }

        Neo4jSyncContext neo4jSyncContext =Neo4jSyncContext.getInstance();
//        relationCqlList.clear();

//        StringBuilderPool   pool=StringBuilderPool.getInstance();
        for (PgWalChangeAck changeAck : dataList) {
            PgWalChange change=changeAck.getChange();
            String tableName = change.getTable();
            AbstractRowEvent row= change.getChangeToRow();
            EventType  eventType=row.getEventType();
            relationParamMap.clear();
            switch (eventType) {
                case INSERT:
                    handleInsertUpdate(  change ,  tableName ,   cqlList);
                    break;
                case UPDATE:
                    if (isDeleteRecord( change)) {
                        handleDelete(  change ,  tableName,  cqlList);
                    }
                    else
                    {
                        handleInsertUpdate(  change ,  tableName,  cqlList) ;
                    }
                    break;
                case DELETE:
                    handleDelete(  change ,  tableName, cqlList);
                    break;
                default:
            }

        }

        if(CollectionUtils.isNotEmpty(cqlList))
        {
            Neo4jDataRepository dataRepository= SpringApplicationContext.getBean(Neo4jDataRepository.class);
            dataRepository.executeBatchCql(cqlList);

//            Neo4jAsyncDataRepository dataRepository= SpringApplicationContext.getBean(Neo4jAsyncDataRepository.class);
//            dataRepository.runAsyncList(relationCqlList);
//            AsyncSession session= dataRepository.getAsyncSession();
//            // 执行CQL语句
//           dataRepository.runAsyncList2(session,relationCqlList);


//            dataRepository.runAsyncList2(cqlList);

//            result.whenComplete((aVoid, throwable) -> {
//                // 处理结果或异常
//                if (throwable != null) {
//                    throwable.printStackTrace();
//                }
//                // 关闭AsyncSession和Driver
//                session.closeAsync().toCompletableFuture().join();
//            });
        }
    }

    @Override
    protected void processDetail(List<PgWalChangeAck> dataList,List<String>  cqlList) {
        try
        {
            handleModelDataRelDetail( dataList,  cqlList);
        }
        catch (Exception e)
        {
            String rawData=null;
            if (CollectionUtils.isNotEmpty(dataList)) {
                PgWalChangeAck  pck=dataList.get(0);
                if(pck!=null&& pck.getChange()!=null)
                {
                    PgWalChange   change=pck.getChange();
                    rawData=change.getrData();
                }

            }
            String exception =null;
            StackTraceElement s= e.getStackTrace()[0];
            if(s!=null)
            {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                e.printStackTrace(new PrintStream(baos));
                exception = baos.toString();
            }

            log.error("WalRecordModelDataRelationHandler handleModelDataRelDetail   error! error line = "+s.getLineNumber() +" ### rawData: "+rawData+" ### ",exception);
            log.error( this+" processDetail error!", ExceptionUtils.toStack(e));
            throw  e ;
        }

    }

    private  void handleDelete( PgWalChange change , String tableName,List<String>  cqlList)
    {
        Neo4jSyncContext neo4jSyncContext =Neo4jSyncContext.getInstance();
        String primaryKey=neo4jSyncContext.getTablePrimaryKey(tableName);
        Column primaryKeyColumn=change.makeOneColumn(primaryKey);
        Object  primaryKeyValue=primaryKeyColumn.getRealValue();
        relationParamMap.put("tableLabel",tableName);
        relationParamMap.put("primaryKey",primaryKey);

        if(primaryKeyValue instanceof  String)
        {
            if (primaryKeyValue!=null) {
                primaryKeyValue= ((String) primaryKeyValue).replace("'", "\\'");
            }

            relationParamMap.put("primaryKeyValue","'"+primaryKeyValue+"'");
        }
        else
        {
            relationParamMap.put("primaryKeyValue",primaryKeyValue);
        }


        StrSubstitutor   sub = new StrSubstitutor  (relationParamMap);
        String  resCql= sub.replace(RELATION_DELETE_CQL);
        cqlList.add(resCql);
    }

    private  void handleMutilRelationDetail( PgWalChange change , String tableName ,  Map<String,List<LogRelationConfig>>    relationMapList,List<String>  cqlList)
    {
        Neo4jSyncContext neo4jSyncContext =Neo4jSyncContext.getInstance();
        String primaryKey=neo4jSyncContext.getTablePrimaryKey(tableName);
        Column primaryKeyColumn=change.makeOneColumn(primaryKey);
        if (primaryKeyColumn == null) {
            log.error(tableName+" primary key  is null ! "+change.toString());
            addCqlErrorLog( change.getSchema(),change.getTable(),1,null, "没有配置主键", change.toString());
        }
        Object  primaryKeyValue=primaryKeyColumn.getRealValue();

        StringBuilder  leftNodeCql=  new StringBuilder("");
        StringBuilder  rightNodeCql=  new StringBuilder("");
        StringBuilder  reidCql=  new StringBuilder("");
        StringBuilder  reFieldCql=  new StringBuilder("");
        INeo4jDataRepository dataRepository =  Neo4jDataRepositoryContext.get(this.getPipelineId());
        for(Map.Entry<String,List<LogRelationConfig>>  entry : relationMapList.entrySet())
        {
            String mapKey=entry.getKey();
            List<LogRelationConfig>  relationList=entry.getValue();
            if (CollectionUtils.isEmpty(relationList)) {
                continue;
            }
            if (relationList.size()==1) {
                LogRelationConfig  logRelationConfig=relationList.get(0);
                // 符合subtable和public条件执行
                if (!DEFAUT_SHCHAME.equals(change.getSchema())&& matchTargetTableFromContext(neo4jSyncContext.getWhiteTableList(),logRelationConfig.getEndTableName())
                        && matchTargetTableFromContext(neo4jSyncContext.getWhiteTableList(),logRelationConfig.getStartTableName())) {
                    handleSimpleRelationDetail(  change ,  tableName,logRelationConfig,  cqlList );
                }
            }
            else
            {
                leftNodeCql.setLength(0);
                rightNodeCql.setLength(0);
                reidCql.setLength(0);
                reFieldCql.setLength(0);

                leftNodeCql.append("   { ");
                rightNodeCql.append("  { ");
                reidCql.append("");
                reFieldCql.append("");
                String leftTable=null;
                String rightTable=null;
                int size=relationList.size();
                for(int k=0;k<size;k++)
                {
                    LogRelationConfig relMap= relationList.get(k);
                    Integer erLabel=relMap.getType();
                    Long reId=  relMap.getId();
                    reidCql.append(reId);
                    String fromTable=leftTable= relMap.getStartTableName();
                    String toTable=rightTable= relMap.getEndTableName();
                    String    startAttrName=relMap.getStartAttrName();
                    String    endAttrName=relMap.getEndAttrName();

                    if(   StringUtils.isBlank(fromTable) ||  StringUtils.isBlank(toTable)
                            || StringUtils.isBlank(startAttrName) ||  StringUtils.isBlank(endAttrName)
                    )
                    {
                        continue ;
                    }


                    Column column=null;

                    if(tableName.equals(fromTable))
                    {
                        column= change.makeOneColumn(startAttrName);
                    }
                    else
                    {
                        column= change.makeOneColumn(endAttrName);
                    }

                    if(column==null)
                    {
                        continue ;
                    }
                    Object  toColumnValue=column.getRealValue();

                    if(toColumnValue  instanceof String)
                    {
                        if (toColumnValue!=null) {
                            toColumnValue= (String)((String) toColumnValue).replace("'", "\\'");
                            toColumnValue="'"+toColumnValue+"'";
                        }

                    }
                    if(toColumnValue==null)
                    {
                        toColumnValue="''";
                    }
                    reFieldCql.append(startAttrName).append("->").append(endAttrName);
                    leftNodeCql.append(" ").append(startAttrName).append(":").append(toColumnValue);
                    rightNodeCql.append(" ").append(endAttrName).append(":").append(toColumnValue);

                    if(k<size-1)
                    {
                        leftNodeCql.append(", ");
                        rightNodeCql.append(", ");
                        reidCql.append(",");
                        reFieldCql.append(",");
                    }


                }

                leftNodeCql.append(" } ");
                rightNodeCql.append(" } ");

                //   CREATE INDEX ON :DER(type,multi_er)
                //CREATE INDEX ON :DER(type,re_ids,multi_er,re_fields)
                // 符合subtable和public条件执行
                if (!DEFAUT_SHCHAME.equals(change.getSchema())&& matchTargetTableFromContext(neo4jSyncContext.getWhiteTableList(),rightTable)
                        && matchTargetTableFromContext(neo4jSyncContext.getWhiteTableList(),leftTable)) {
                    StringBuilder  deleteCql=  new StringBuilder("  match ( left:").append(leftTable).append(leftNodeCql.toString()).append(" ) ");
                    deleteCql.append("   -[r:DER { type:'1000' ,multi_er:'1' } ] -> ") ;
                    deleteCql.append(" ( right:").append(rightTable).append(rightNodeCql.toString()).append(" )  ");
                    deleteCql.append( "  delete r ");
                    //                relationCqlList.add(deleteCql.toString());
                    //                dataRepository.operNeo4jDataByCqlSingle(deleteCql.toString());
                    dataRepository.executeOneCqlWithSimple(deleteCql.toString());

                    StringBuilder  createCql=  new StringBuilder("  match ( left:").append(leftTable).append(leftNodeCql.toString()).append(" ) , ");
                    createCql.append(" ( right:").append(rightTable).append(rightNodeCql.toString()).append(" )  ");
                    // 关于自引用边，设置属性值
                    String selfReferenced = ",SelfR:false";
                    if (leftTable.equals(rightTable)) {
                        selfReferenced = ",SelfR:true";
                    }
                    createCql.append("  merge (left) -[r:DER { type:'1000' ,re_ids:'").append(reidCql).append("',multi_er:'1',re_fields:'").append(reFieldCql.toString()).append("'").append(selfReferenced).append(" } ] ->(right) ") ;

                    String  leftNodeCqlInfo=leftNodeCql.toString().trim();
                    String  rightNodeCqlInfo=rightNodeCql.toString().trim();
                    if(StringUtils.isBlank(leftNodeCqlInfo)
                            || StringUtils.isBlank(rightNodeCqlInfo))
                    {
                        log.error("relation config error! rel: "+ relationParamMap.toString()+"   #########  cql:  "+createCql);
                        addCqlErrorLog( change.getSchema(),change.getTable(),2,createCql.toString(), "关系配错", change.toString());
                    }
                    cqlList.add(createCql.toString());
                }
            }


        }
        //主动释放，服务器太烂了
        leftNodeCql=null;
        rightNodeCql=null;
        reidCql=null;
        reFieldCql=null;

    }

    private void addCqlErrorLog(String schema,String tableName,Integer errorType,String cql, String errorDesc,String walChange)
    {
        Object[]  parameters={schema, tableName, errorType, cql,  errorDesc, walChange};
        DbOperateUtil.insert(DataSynConstant.INSERT_CQL_ERROR_LOG_CQL,parameters);
    }

    private  void handleSimpleRelationDetail( PgWalChange change , String tableName ,  LogRelationConfig  logRelationConfig,List<String>  cqlList)
    {
//        INeo4jDataRepository dataRepository =  Neo4jDataRepositoryContext.get(this.getPipelineId());
        Neo4jSyncContext neo4jSyncContext =Neo4jSyncContext.getInstance();
        String primaryKey=neo4jSyncContext.getTablePrimaryKey(tableName);
        Column primaryKeyColumn=change.makeOneColumn(primaryKey);

        if (primaryKeyColumn == null) {
            log.error(tableName+" primary key  is null ! "+change.toString());
            addCqlErrorLog( change.getSchema(),change.getTable(),1,null, "没有配置主键", change.toString());
        }

        Object  primaryKeyValue=primaryKeyColumn.getRealValue();
        Integer erLabel=logRelationConfig.getType();
        if(!checkRelList.contains(erLabel))
        {
            return ;
        }

        Long reId=  logRelationConfig.getId();
        String fromTable= logRelationConfig.getStartTableName();
        String toTable= logRelationConfig.getEndTableName();
        String type=logRelationConfig.getRestriction();
        String    startAttrName=logRelationConfig.getStartAttrName();
        String    endAttrName=logRelationConfig.getEndAttrName();

        if(   StringUtils.isBlank(fromTable) ||  StringUtils.isBlank(toTable)
                || StringUtils.isBlank(startAttrName) ||  StringUtils.isBlank(endAttrName)
        )
        {
            return;
        }

        String otherPrimaryKey=null;
        String otherTableName=null;
        Column column=null;
        if(tableName.equals(fromTable))
        {
            column= change.makeOneColumn(startAttrName);
            otherPrimaryKey=neo4jSyncContext.getTablePrimaryKey(toTable);
            otherTableName=toTable;
        }
        else if(tableName.equals(toTable))
        {
            column= change.makeOneColumn(endAttrName);
            otherPrimaryKey=neo4jSyncContext.getTablePrimaryKey(fromTable);
            otherTableName=fromTable;
        }

        if(column==null)
        {
            return;
        }
        Object  toColumnValue=column.getRealValue();

        if(toColumnValue==null||primaryKeyValue==null)
        {
            return;
        }
        if(toColumnValue  instanceof String)
        {
            toColumnValue="'"+toColumnValue+"'";
        }
        if(toColumnValue==null)
        {
            toColumnValue="''";
        }

        relationParamMap.clear();
        relationParamMap.put("fromLabel",fromTable);
        relationParamMap.put("reId",reId);
        relationParamMap.put("toLabel",toTable);
        relationParamMap.put("primaryKey",primaryKey);
        if(primaryKeyValue  instanceof  String)
        {
            relationParamMap.put("primaryKeyValue","'"+primaryKeyValue+"'");
        }
        else
        {
            relationParamMap.put("primaryKeyValue",primaryKeyValue);
        }



        relationParamMap.put("fromField",startAttrName);
        relationParamMap.put("toField",endAttrName);
        relationParamMap.put("erType",type);
        relationParamMap.put("toColumn",endAttrName);
        relationParamMap.put("toColumnValue",toColumnValue);

        String  reFields=  logRelationConfig.getStartAttrName()+">"+logRelationConfig.getEndAttrName();
        String  reIds=  String.valueOf(logRelationConfig.getId());

        relationParamMap.put("reFields",reFields);

        relationParamMap.put("reIds",reIds);
        relationParamMap.put("type","2000");
        relationParamMap.put("multiEr","0");
//      自引用标识默认为false
        relationParamMap.put("selfReferenced",false);
        // 自引用标识添加
        if (fromTable.equals(toTable)) {
            relationParamMap.put("selfReferenced",true);
        }
        StrSubstitutor   sub = new StrSubstitutor  (relationParamMap);
        String  deleteCql= sub.replace(RELATION_UPSERT_DELETE_CQL);
//            relationCqlList.add(deleteCql);
//            dataRepository.operNeo4jDataByCqlSingle(deleteCql.toString(),false);

        sub = new StrSubstitutor(relationParamMap);
        //
        String  resCql= sub.replace(RELATION_UPSERT_CQL);
        cqlList.add(resCql);
        relationParamMap.clear();


    }


    private  void handleInsertUpdate( PgWalChange change , String tableName ,List<String>  cqlList )
    {
        Neo4jSyncContext neo4jSyncContext =Neo4jSyncContext.getInstance();
        Map<String,List<LogRelationConfig>>    mutilRelationMapList= neo4jSyncContext.getModelDataConfigRelationByTable(tableName,ERTYPE_MUTIL);
        if(MapUtils.isNotEmpty(mutilRelationMapList))
        {
            handleMutilRelationDetail(  change ,  tableName,mutilRelationMapList ,cqlList);
        }
//        else
//        {
//            Map<String,List<LogRelationConfig>>  simpleRelationMapList= neo4jSyncContext.getModelDataConfigRelationByTable(tableName,ERTYPE_SIMPLE);
//            if(MapUtils.isNotEmpty(simpleRelationMapList))
//            {
//                handleSimpleRelationDetail(  change ,  tableName,simpleRelationMapList );
//            }
//
//        }

    }

//    private void checkOtherTableNode(String otherTableName,String otherPrimaryKey,Object  toColumnValue)
//    {
//        relationParamMap.clear();
//        relationParamMap.put("primaryKey",otherPrimaryKey);
//        relationParamMap.put("primaryKeyValue",toColumnValue);
//        relationParamMap.put("tableLabel",otherTableName);
//        StringSubstitutor sub = new StringSubstitutor(relationParamMap);
//        String  resCql= sub.replace(NODE_MISS_CQL);
//        relationParamMap.clear();
//        INeo4jDataRepository dataRepository =  Neo4jDataRepositoryContext.get(this.getPipelineId());
//        dataRepository.operNeo4jDataByCqlSingleNoEx(resCql);
//
//    }

    @Override
    public String name() {
        return RECORD_MODEL_DATA_RELATION_DATA_HANDLER;
    }

    private boolean matchTargetTableFromContext(List<String> list,String match) {
        boolean result = false;
        if (CollectionUtils.isEmpty(list)||StringUtils.isEmpty(match)) {
            return result;
        }
        for (String tag : list) {
            if (tag.matches(".*"+match)) {
                result = true;
            }
        }
        return result;
    }
}
