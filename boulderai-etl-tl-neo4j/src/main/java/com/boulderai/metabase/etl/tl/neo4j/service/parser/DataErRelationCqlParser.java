//package com.boulderai.metabase.sync.core.service.parser;
//
//import com.boulderai.metabase.sync.core.config.TableToBeanConfig;
//import com.boulderai.metabase.sync.core.service.wal.model.Column;
//import com.boulderai.metabase.sync.core.service.wal.model.PgWalChange;
//import com.boulderai.metabase.sync.core.util.DataSynConstant;
//import com.boulderai.metabase.sync.core.util.DbOperateUtil;
//import org.apache.commons.text.StringSubstitutor;
//
//import java.util.HashMap;
//import java.util.Map;
//
//
///**
// * @ClassName: ErRelationCqlParser
// * @Description: ER关系关系cql解析适配类
// * @author  df.l
// * @date 2022年10月12日
// * @Copyright boulderaitech.com
// */
//public class DataErRelationCqlParser extends   BaseRelationCqlParser{
//    @Override
//    public String getRawInsertCql() {
//        return ER_INSERT_CQL;
//    }
//
//    @Override
//    public String getRawUpdateCql() {
//        return ER_INSERT_CQL;
//    }
//
//    @Override
//    public String getRawDeleteCql() {
//        return ER_DELETE_CQL;
//    }
//
//    @Override
//    public RelationType getRelationType() {
//        return RelationType.ER;
//    }
//
//
//    @Override
//    public String parseInsertCql(TableToBeanConfig beanConfig , PgWalChange change ) {
//        //match (m:Module { moduleId:${sModuleId}}),(m1 {moduleId:${eModuleId} }) merge (m)-[r:ER {from:${fromField},to:${toField},type:${erType}}]->(m1)
//        String  cql=this.getRawInsertCql();
//        Map<String,Object> valuesMap = new HashMap<String,Object>(6);
//        addStartEndFieldInfo(  cql, beanConfig ,  change , valuesMap);
//        return  parseDetailCql(  cql, beanConfig ,  change ,valuesMap);
//
//    }
//
//    @Override
//    public String parseUpdateCql(TableToBeanConfig beanConfig , PgWalChange change ) {
//        return  parseInsertCql(  beanConfig ,  change );
//    }
//
//    @Override
//    public String parseDeleteCql(TableToBeanConfig beanConfig , PgWalChange change ) {
//        String  cql=  getRawDeleteCql( );
//        Map<String,Object> valuesMap = new HashMap<String,Object>(6);
//        addStartEndFieldInfo(  cql, beanConfig ,  change , valuesMap);
//        return  parseDetailCql(  cql, beanConfig ,  change ,valuesMap);
//    }
//
//    private   void addStartEndFieldInfo(String  cql,TableToBeanConfig beanConfig , PgWalChange change ,Map<String,Object> valuesMap)
//    {
//        setColumnValue2Map( "restriction","erType", change , valuesMap,true);
//
//        Column startColumn=  change.makeOneColumn("start_attr_id");
//        Column endColumn=  change.makeOneColumn("end_attr_id");
//
//        if(startColumn==null||endColumn==null)
//        {
//            return ;
//        }
//        Object  startFieldValue=startColumn.getRealValue();
//        Object  endFieldValue=endColumn.getRealValue();
//        if(startFieldValue==null||endFieldValue==null)
//        {
//            return ;
//        }
//
//        Map<String, Object> startFieldMap = DbOperateUtil.queryMap(DataSynConstant.DA_ENTITY_ATTRIBUTE_SQL,startFieldValue);
//       if(startFieldMap!=null&&!startFieldMap.isEmpty())
//       {
//           String code= (String) startFieldMap.getOrDefault("code","");
//           valuesMap.put("fromField",code);
//
//       }
//       else
//       {
//           valuesMap.put("fromField","");
//       }
//
//        Map<String, Object> endFieldMap = DbOperateUtil.queryMap(DataSynConstant.DA_ENTITY_ATTRIBUTE_SQL,endFieldValue);
//
//        if(endFieldMap!=null&&!endFieldMap.isEmpty())
//        {
//            String code= (String) endFieldMap.getOrDefault("code","");
//            valuesMap.put("toField",code);
//        }
//        else {
//            valuesMap.put("toField","");
//        }
//    }
//
//    protected String  parseDetailCql(String  cql,TableToBeanConfig beanConfig , PgWalChange change ,Map<String,Object> valuesMap)
//    {
//        setColumnValue2Map( "start_id","sModuleId", change , valuesMap,true);
//        setColumnValue2Map( "end_id","eModuleId", change , valuesMap,true);
//
//
//        StringSubstitutor sub = new StringSubstitutor(valuesMap);
//        String  resCql= sub.replace(cql);
//        valuesMap.clear();
//        return resCql;
//    }
//}
