//package com.boulderai.metabase.sync.core.service.handler.pg;
//
//import com.boulderai.metabase.sync.core.service.handler.BaseDataHandler;
//import com.boulderai.metabase.sync.core.service.pipeline.PipelineContext;
//import com.boulderai.metabase.sync.core.service.wal.model.*;
//import com.boulderai.metabase.sync.core.util.DbOperateUtil;
//
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
//public class PgPersistDataHandler  extends BaseDataHandler {
//
//    private final Map<String,String> pgOperaSqlMap=new ConcurrentHashMap<>(10);
//    private final Map<String,SqlValueContainer > sqlParameterMap=new ConcurrentHashMap<String,SqlValueContainer  >(20);
//    private final  LinkedList<String>  sqlQueue=new LinkedList<String>();
//
//    private void caclParameters(PgWalChange  change,String  sqlKey,String sql,String schema,String  tableName)
//    {
//        Object[] parameter=null;
//        if (change.getKind().equals(EventType.INSERT.toString())) {
//            parameter= this.makeInsertParameter( schema,  tableName,change);
//        }
//        else  if (change.getKind().equals(EventType.UPDATE.toString())) {
//            parameter= this.makeUpdateParameter(schema,  tableName,change);
//        }
//        else  if (change.getKind().equals(EventType.DELETE.toString())) {
//            parameter= this.makeDeleteParameter(schema,  tableName,change);
//        }
//
//        SqlValueContainer  container =  sqlParameterMap.get(sqlKey);
//        if(container==null)
//        {
//            container=new SqlValueContainer(sql);
//            sqlParameterMap.put(sqlKey,container);
//        }
//        container.setSql(sql);
//        container.add(parameter);
//    }
//
//    @Override
//    protected void processDetail(List<PgWalChange> dataList) {
//        for(SqlValueContainer list : sqlParameterMap.values())
//        {
//            list.clear();
//        }
//        sqlQueue.clear();
//
//        for(PgWalChange  change: dataList)
//        {
//            String  kind=change.getKind();
//            String  tableName=change.getTable();
//            String  schema=change.getSchema();
//            String  sqlKey=schema+"."+tableName+"#"+kind;
//            String  sql=pgOperaSqlMap.get(sqlKey);
//            sqlQueue.add(sqlKey);
//
//            if(sql==null)
//            {
//                if (kind.equals(EventType.INSERT.toString())) {
//                    sql= makeInsertSql( schema,  tableName,  change);
//                    pgOperaSqlMap.put(sqlKey,sql);
//                } else if (kind.equals(EventType.UPDATE.toString())) {
//                    sql= makeUpdateSql( schema,  tableName,  change);
//                    pgOperaSqlMap.put(sqlKey,sql);
//                } else if (kind.equals(EventType.DELETE.toString())) {
//                    sql= makeDeleteSql( schema,  tableName,  change);
//                    pgOperaSqlMap.put(sqlKey,sql);
//                }
//            }
//            caclParameters(  change,  sqlKey, sql,schema,  tableName);
//        }
//
//        //按照顺序执行防止错乱
//        while(!sqlQueue.isEmpty())
//        {
//            String sqlKey= sqlQueue.remove();
//            SqlValueContainer  container =  sqlParameterMap.get(sqlKey);
//            if(container!=null)
//            {
//                DbOperateUtil.batchUpdate(container.getSql(),container.getParameterList());
//            }
//        }
//    }
//
//
//    private   String makeDeleteSql(String schema,String  tableName,PgWalChange  change)
//    {
//        StringBuilder  sqlBuilder=new StringBuilder(" delete from   ").append(schema).append(".").append(tableName).append(" where ");
//        WalChangeOldKey  walOldKeys=change.getOldkeys();
//        List<String> keyNames=walOldKeys.getKeynames();
//        StringJoiner whereJoiner = new StringJoiner(" and ");
//        keyNames.forEach(columnName -> {
//            if(PipelineContext.hasParentTableColumn(tableName,columnName))
//            {
//                whereJoiner.add(columnName+" = ? ");
//            }
//
//        });
//
//        sqlBuilder.append(whereJoiner.toString()).append("   ");
//        return sqlBuilder.toString();
//    }
//
//    private   Object[]  makeDeleteParameter(String schema,String  tableName,PgWalChange  change){
//        List<Column>  columnList=change.makeColumn();
//        List<Object>  parameters=new ArrayList<Object>(columnList.size());
//
//        List<String> keyNames=change.getOldkeys().getKeynames();
//        keyNames.forEach(columnName -> {
//            for(Column  column : columnList)
//            {
//                if(columnName.equals(column.getName())
//                  && PipelineContext.hasParentTableColumn(tableName,columnName) )
//                {
//                    parameters.add(column.getRealValue());
//                    break;
//                }
//            }
//        });
//
//        return parameters.toArray(new Object[parameters.size()]);
//    }
//
//    private   String makeUpdateSql(String schema,String  tableName,PgWalChange  change)
//    {
//        StringBuilder  sqlBuilder=new StringBuilder(" update  ").append(schema).append(".").append(tableName).append(" set ");
//        List<String> columns=change.getColumnnames();
//        WalChangeOldKey  oldKeys=change.getOldkeys();
//        StringJoiner columnJoiner = new StringJoiner(",");
//        columns.forEach(columnName -> {
//            if(!oldKeys.getKeynames().contains(columnName)
//                    && PipelineContext.hasParentTableColumn(tableName,columnName) )
//            {
//                columnJoiner.add(columnName+" = ? ");
//            }
//        });
//        sqlBuilder.append(columnJoiner.toString()).append("  where ");
//
//
//        List<String> keyNames=oldKeys.getKeynames();
//        StringJoiner whereJoiner = new StringJoiner(" and ");
//        keyNames.forEach(columnName -> {
//            if(PipelineContext.hasParentTableColumn(tableName,columnName))
//            {
//                whereJoiner.add(columnName+" = ? ");
//            }
//
//        });
//
//        sqlBuilder.append(whereJoiner.toString()).append("   ");
//        return sqlBuilder.toString();
//    }
//
//    private   Object[]  makeUpdateParameter(String schema,String  tableName, PgWalChange  change){
//        List<Column>  columnList=change.makeColumn();
//        List<String> keyColumnNames  =change.getOldkeys().getKeynames();
//        List<Object>  parameters=new ArrayList<Object>(columnList.size());
//        columnList.forEach(column -> {
//            if(!keyColumnNames.contains(column.getName())
//                && PipelineContext.hasParentTableColumn(tableName,column.getName()) )
//            {
//                parameters.add(column.getRealValue());
//            }
//
//        });
//
//        List<String> keyNames=change.getOldkeys().getKeynames();
//        keyNames.forEach(columnName -> {
//             for(Column  column : columnList)
//             {
//                 if(columnName.equals(column.getName())
//                     && PipelineContext.hasParentTableColumn(tableName,columnName) )
//                 {
//                     parameters.add(column.getRealValue());
//                     break;
//                 }
//             }
//        });
//
//        return parameters.toArray(new Object[parameters.size()]);
//    }
//
//    private   String makeInsertSql(String schema,String  tableName,PgWalChange  change)
//    {
//        StringBuilder  sqlBuilder=new StringBuilder(" insert into  ").append(schema).append(".").append(tableName).append(" ( ");
//        List<String> columns=change.getColumnnames();
//        StringJoiner columnJoiner = new StringJoiner(",");
//        StringJoiner parameterJoiner = new StringJoiner(",");
//        columns.forEach(columnName -> {
//            if(PipelineContext.hasParentTableColumn(tableName,columnName))
//            {
//                columnJoiner.add(columnName);
//                columnJoiner.add(" ? ");
//            }
//        });
//        sqlBuilder.append( "  " ).append(columnJoiner.toString()).append(" )  values ( ");
//        sqlBuilder.append(parameterJoiner.toString());
//        sqlBuilder.append(" ) ");
//        return sqlBuilder.toString();
//    }
//
//
//    private   Object[]  makeInsertParameter(String schema,String  tableName,PgWalChange  change){
//        List<Column>  columnList=change.makeColumn();
//        List<Object>  parameters=new ArrayList<Object>(columnList.size());
//        columnList.forEach(column -> {
//            if(PipelineContext.hasParentTableColumn(tableName,column.getName()))
//            {
//                parameters.add(column.getRealValue());
//            }
//        });
//
//        return parameters.toArray(new Object[parameters.size()]);
//    }
//
//    @Override
//    public String name() {
//        return PG_PERSIST_DATA_HANDLER_NAME;
//    }
//}
