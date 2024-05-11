package com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.model;

import com.boulderai.metabase.etl.tl.neo4j.service.handler.BaseDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.IDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.Neo4jSyncContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.*;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @ClassName: PersistModelDataDataHandler
 * @Description: 模型节点数据持久化处理器
 * @author  df.l
 * @date 2022年10月26日
 * @Copyright boulderaitech.com
 */
@Slf4j
public class PersistModelDataDataHandler extends BaseDataHandler {

    private static final Gson gson = new Gson();
    private static AtomicInteger count =new AtomicInteger(0);
    /**
     * table+oper, cql
     */
//    private List<String> neo4jOperCqlList=new ArrayList<String>(10);

    private final static  Neo4jSyncContext   neo4jSyncContext =Neo4jSyncContext.getInstance();

    public  PersistModelDataDataHandler()
    {
        this.relogError=false;
    }


    @Override
    protected void processDetail(List<PgWalChangeAck> dataList, List<String>  cqlList) {

        try
        {
            handleModelDataDetail( dataList,cqlList);
        }
        catch (Exception e)
        {
            log.error("PersistModelDataDataHandler handleModelDataDetail error!",e);
            throw  e ;
        }

    }

    //        for (PgWalChangeAck changeAck : dataList) {
//            PgWalChange change=changeAck.getChange();

    protected void handleModelDataDetail(List<PgWalChangeAck> dataList,List<String>  cqlList) {
//        neo4jOperCqlList.clear();

        /**
         * 批量构造本次修改行
         */
        for (PgWalChangeAck changeAck : dataList) {
            PgWalChange walChange=changeAck.getChange();
            String  tableName=walChange.getTable();
            if(!neo4jSyncContext.hasLogicEntityConfig(tableName))
            {
                continue;
            }

            AbstractRowEvent row= walChange.getChangeToRow();
            EventType eventType=row.getEventType();
            String  eventTypeStr=eventType.toString();
            String  cql=null;


            //insert事件
            if(EventType.INSERT.toString().equals(eventTypeStr))
            {
                cql=   makeNewInsertCql(  tableName,  row,walChange);
            }
            //update事件
            else     if(EventType.UPDATE.toString().equals(eventTypeStr))
            {
                if (isDeleteRecord( walChange)) {
                    cql=   makeDeleteCql(  tableName,   row,walChange);
                }
                else
                {
                    cql=   makeUpdateCql(  tableName,  row,walChange);
                }

            }

            //delete事件
            else   if(EventType.DELETE.toString().equals(eventTypeStr))
            {
                cql=   makeDeleteCql(  tableName,   row,walChange);
            }

            if(StringUtils.isNotBlank(cql))
            {
//                neo4jOperCqlList.add(cql);
                cqlList.add(cql);
            }

        }

        if(!cqlList.isEmpty())
        {
//            INeo4jDataRepository dataRepository = Neo4jDataRepositoryContext.get(this.getPipelineId());
//            dataRepository.executeBatchCql(cqlList);
            //            for(String   cql : neo4jOperCqlList)
//            {
//                dataRepository.operNeo4jDataByCqlSingle(cql);
//            }

//            Neo4jAsyncDataRepository dataRepository= SpringApplicationContext.getBean(Neo4jAsyncDataRepository.class);
            //
//            count.addAndGet(neo4jOperCqlList.size());
//            dataRepository.runAsyncList2(cqlList);

//            log.info("##### total  node  write count "+ count.get());





        }

    }



    @Override
    public String name() {
        return IDataHandler.PERSIST_MODEL_DATA_HANDLER_NAME;
    }

    @Override
    public void  close()
    {
//        neo4jOperCqlList.clear();
    }



    /**
     * 生成insert  cql语句
     * @param tableName  表名
     * @param row 数据变化事件
     * @return  cql
     */
    private   String makeNewInsertCql(String  tableName,AbstractRowEvent  row,PgWalChange walChange)
    {
//            StringBuilderPool pool=StringBuilderPool.getInstance();
//            StringBuilder  cqlBuilder=pool.borrowObject();
           StringBuilder  cqlBuilder=new  StringBuilder();

             List<Column>  allColumns= walChange.makeColumn();
             Map<String,Object>  columnMap=new HashMap<String,Object>();
             allColumns.forEach(column->{
                 if(column.getRealValue()!=null)
                 {
                     columnMap.put(column.getName(), column.getRealValue());
                 }
             });

            //创建cql
            cqlBuilder.append(" MERGE ( t:").append(tableName).append(" { ");
            //MERGE   (p:Person{name:"zhangsan"})
            String  primaryKeyName  = neo4jSyncContext.getTablePrimaryKey(tableName);
            cqlBuilder.append(primaryKeyName).append(":") ;
            Object  kValue=columnMap.get(primaryKeyName);
            if(kValue instanceof  String)
            {
                cqlBuilder.append("'");
                cqlBuilder.append(kValue);
                cqlBuilder.append("'");
            }
            else
            {
                cqlBuilder.append(kValue);
            }

            if(kValue==null)
            {
                cqlBuilder.setLength(0);
//                pool.returnObject(cqlBuilder);
                return  null;
            }
            cqlBuilder.append(" })   " );
            cqlBuilder.append("      on CREATE  set t=  ") ;
            String  allColumnMaps=gson.toJson(columnMap);
           String reJson = allColumnMaps.replaceAll("\"(\\w+)\"(\\s*:\\s*)", "$1$2");
          cqlBuilder.append(reJson);
            cqlBuilder.append("  \n\t on MATCH  set  t= ");
           cqlBuilder.append(reJson);

            String statement=cqlBuilder.toString();
            //保存起来重用
//            pool.returnObject(cqlBuilder);
           cqlBuilder=null;
            return statement;

    }




    /**
     * 生成update  cql语句
     * @param tableName  表名
     * @param row 数据变化事件
     * @return  cql
     */
    private   String makeUpdateCql(String  tableName,AbstractRowEvent  row,PgWalChange walChange)
    {
//        StringBuilderPool pool=StringBuilderPool.getInstance();
//        StringBuilder  cqlBuilder=pool.borrowObject();
        StringBuilder  cqlBuilder=new StringBuilder();

        List<Column>  allColumns= walChange.makeColumn();
        Map<String,Object>  columnMap=new HashMap<String,Object>();
        allColumns.forEach(column->{
            columnMap.put(column.getName(), column.getRealValue());
        });

        //创建cql
        cqlBuilder.append(" MERGE ( t:").append(tableName).append(" { ");
        //MERGE   (p:Person{name:"zhangsan"})
        String  primaryKeyName  = neo4jSyncContext.getTablePrimaryKey(tableName);
        if(primaryKeyName==null)
        {
            primaryKeyName= walChange.getOldkeys().getKeynames().get(0);
        }
        cqlBuilder.append(primaryKeyName).append(":") ;
        Object  kValue=columnMap.get(primaryKeyName);
        if(kValue instanceof  String)
        {
            cqlBuilder.append("'");
            cqlBuilder.append(kValue);
            cqlBuilder.append("'");
        }
        else
        {
            cqlBuilder.append(kValue);
        }
        if(kValue==null)
        {
            cqlBuilder.setLength(0);
//            pool.returnObject(cqlBuilder);
            return  null;
        }

        cqlBuilder.append(" })   " );
        cqlBuilder.append("      on CREATE  set t=  ") ;
        String  allColumnMaps=gson.toJson(columnMap);
        String reJson = allColumnMaps.replaceAll("\"(\\w+)\"(\\s*:\\s*)", "$1$2");
        cqlBuilder.append(reJson);

        cqlBuilder.append("  \n\t on MATCH  set  t= ");
        cqlBuilder.append(reJson);
        String statement=cqlBuilder.toString();
//        pool.returnObject(cqlBuilder);
        cqlBuilder=null;
        return statement;
    }


    /**
     * 生成delete  cql语句
     * @param tableName  表名
     * @param row 数据变化事件
     * @return  cql
     */
    private   String makeDeleteCql(String  tableName,AbstractRowEvent  row,PgWalChange walChange)
    {
        String  primaryKeyName  = neo4jSyncContext.getTablePrimaryKey(tableName);
        if(primaryKeyName==null)
        {
            primaryKeyName= walChange.getOldkeys().getKeynames().get(0);
        }
//        StringBuilderPool pool=StringBuilderPool.getInstance();
//        StringBuilder  cqlBuilder=pool.borrowObject();
        StringBuilder  cqlBuilder=new StringBuilder();
        //delete  cql
        cqlBuilder.append(" MATCH  ( t:").append(tableName).append(" { ");
        cqlBuilder.append(primaryKeyName).append(":") ;
        // todo oldkeys 为空，空指针
        Column  column= walChange.getOldkeys().makeOneColumn(primaryKeyName);
        Object  kValue=column.getRealValue();
        if(kValue instanceof  String)
        {
            cqlBuilder.append("'");
            cqlBuilder.append(kValue);
            cqlBuilder.append("'");
        }
        else
        {
            cqlBuilder.append(kValue);
        }

        if(kValue==null)
        {
            cqlBuilder.setLength(0);
//            pool.returnObject(cqlBuilder);
            return  null;
        }
        cqlBuilder.append(" }) ");
        cqlBuilder.append(" DETACH DELETE t ");
        String statement=cqlBuilder.toString();
//        pool.returnObject(cqlBuilder);
        cqlBuilder=null;
        return statement;
    }


}
