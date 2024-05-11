//package com.boulderai.metabase.sync.core.service.parser;
//
//import com.boulderai.metabase.sync.core.config.TableToBeanConfig;
//import com.boulderai.metabase.sync.core.service.wal.model.Column;
//import com.boulderai.metabase.sync.core.service.wal.model.PgWalChange;
//import org.apache.commons.text.StringSubstitutor;
//
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * @ClassName: SbrRelationCqlParser
// * @Description: Ser关系关系cql解析适配类
// * @author  df.l
// * @date 2022年10月12日
// * @Copyright boulderaitech.com
// */
//public class SerRelationCqlParser extends   BaseRelationCqlParser{
//    @Override
//    public String getRawInsertCql() {
//        return SER_INSERT_CQL;
//    }
//
//    @Override
//    public String getRawUpdateCql() {
//        return getRawInsertCql();
//    }
//
//    @Override
//    public String getRawDeleteCql() {
//        return SER_DELETE_CQL;
//    }
//
//    @Override
//    public RelationType getRelationType() {
//        return RelationType.SER;
//    }
//
//
//    @Override
//    public String parseInsertCql(TableToBeanConfig beanConfig , PgWalChange change ) {
//        //match (m:Module {moduleId:'${sModuleId}' }),(m1:Module {moduleId:'${eModuleId}' }) merge (m)-[r:SER {sceneKey:'${sceneKey}'  } ]->(m1)
//        String  cql=this.getRawInsertCql();
//        Map<String,Object> valuesMap = new HashMap<String,Object>(6);
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
//        return  parseDeleteDetailCql(  cql, beanConfig ,  change ,valuesMap);
//    }
//
//
//
//
//    protected String  parseDetailCql(String  cql,TableToBeanConfig beanConfig , PgWalChange change ,Map<String,Object> valuesMap)
//    {
//        setColumnValue2Map( "scene_code","sceneKey", change , valuesMap,true);
//        setColumnValue2Map( "restriction","erType", change , valuesMap,true);
//        setColumnValue2Map( "start_attr_code","fromField", change , valuesMap,true);
//        setColumnValue2Map( "end_attr_code","toField", change , valuesMap,true);
//        setColumnValue2Map( "startid","sModuleId", change , valuesMap,true);
//        setColumnValue2Map( "endid","eModuleId", change , valuesMap,true);
//        setColumnValue2Map( "id","reId", change , valuesMap,true);
//        return mergeCql( cql, valuesMap);
//    }
//
//
//
//}
