package com.boulderai.metabase.etl.tl.neo4j.service.parser;

import com.boulderai.metabase.lang.er.RelationType;
import com.boulderai.metabase.etl.tl.neo4j.config.TableToBeanConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import org.apache.commons.text.StringSubstitutor;

import java.util.HashMap;
import java.util.Map;


/**
 * @ClassName: FerRelationCqlParser
 * @Description: ER关系关系cql解析适配类
 * @author  df.l
 * @date 2022年10月12日
 * @Copyright boulderaitech.com
 */
public class FerRelationCqlParser extends   BaseRelationCqlParser{
    @Override
    public String getRawInsertCql() {
        return FER_INSERT_CQL;
    }

    @Override
    public String getRawUpdateCql() {
        return FER_INSERT_CQL;
    }

    @Override
    public String getRawDeleteCql() {
        return FER_DELETE_CQL;
    }

    @Override
    public RelationType getRelationType() {
        return RelationType.FER;
    }


    @Override
    public String parseInsertCql(TableToBeanConfig beanConfig , PgWalChange change ) {
        // match  (f:Field {id: '${fieldId}' }),  (m:Module {moduleId:'${moduleId}' }) merge (f)-[r:FMR {  reId:'${reId}' }]->(m)
        String  cql=this.getRawInsertCql();
        Map<String,Object> valuesMap = new HashMap<String,Object>(6);
//        addStartEndFieldInfo(  cql, beanConfig ,  change , valuesMap);
        return  parseDetailCql(  cql, beanConfig ,  change ,valuesMap);

    }

    @Override
    public String parseUpdateCql(TableToBeanConfig beanConfig , PgWalChange change ) {
        return  parseInsertCql(  beanConfig ,  change );
    }

    @Override
    public String parseDeleteCql(TableToBeanConfig beanConfig , PgWalChange change ) {
        // match ( f:Field { id: '${fieldId}' }) -[r:FMR { reId:'${reId}' }]->  ( m:Module }) delete r
        String  cql=  getRawDeleteCql( );
        Map<String,Object> valuesMap = new HashMap<String,Object>(6);
        setColumnValue2Map( "id","fieldId", change , valuesMap,true,false);
        return  parseDeleteDetailCql(  cql, beanConfig ,  change ,valuesMap);
    }


    protected String  parseDetailCql(String  cql,TableToBeanConfig beanConfig , PgWalChange change ,Map<String,Object> valuesMap)
    {
        setColumnValue2Map( "id","fieldId", change , valuesMap,true,false);
        setColumnValue2Map( "entity_id","moduleId", change , valuesMap,true,false);
        setColumnValue2Map( "id","reId", change , valuesMap,true,false);
        setColumnValue2Map( "options","options", change , valuesMap,true,true);
        StringSubstitutor sub = new StringSubstitutor(valuesMap);
        String  resCql= sub.replace(cql);
        valuesMap.clear();
        return resCql;
    }
}
