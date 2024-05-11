package com.boulderai.metabase.etl.tl.neo4j.service.parser;

import com.boulderai.metabase.lang.er.RelationType;
import com.boulderai.metabase.etl.tl.neo4j.config.TableToBeanConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: SfmrRelationCqlParser
 * @Description: sfmr关系关系cql解析适配类
 * @author  df.l
 * @date 2022年10月12日
 * @Copyright boulderaitech.com
 */
public class SfmrRelationCqlParser extends   BaseRelationCqlParser{
    @Override
    public String getRawInsertCql() {
        return SFMR_INSERT_CQL;
    }

    @Override
    public String getRawUpdateCql() {
        return getRawInsertCql();
    }

    @Override
    public String getRawDeleteCql() {
        return SFMR_DELETE_CQL;
    }

    @Override
    public RelationType getRelationType() {
        return RelationType.SFMR;
    }

    protected String  parseDetailCql(String  cql,TableToBeanConfig beanConfig , PgWalChange change ,Map<String,Object> valuesMap)
    {
        setColumnValue2Map( "scene_code","sceneKey", change , valuesMap,true,false);
        setColumnValue2Map( "logic_id","moduleId", change , valuesMap,true,false);
        setColumnValue2Map( "attr_id","fieldId", change , valuesMap,true,false);
        return mergeCql( cql, valuesMap);
    }

    @Override
    public String parseInsertCql(TableToBeanConfig beanConfig , PgWalChange change ) {
        //match (m:Module{moduleId:$moduleId}),(f:Field{id:$fieldId}) MERGE (f)-[r:SFMR {sceneKey:$sceneKey}]->(m)
        //match (m:Module{moduleId:'${moduleId}' }),(f:Field{id:'${fieldId}' }) MERGE (f)-[r:SFMR {sceneKey:'${sceneKey}',reId:'${reId}' }]->(m)
        String  cql=this.getRawInsertCql();
        Map<String,Object> valuesMap = new HashMap<String,Object>(2);
        setColumnValue2Map( "id","reId", change , valuesMap,true,false);
        return  parseDetailCql(  cql, beanConfig ,  change ,valuesMap);
    }

    @Override
    public String parseUpdateCql(TableToBeanConfig beanConfig , PgWalChange change ) {
        return parseInsertCql( beanConfig ,  change ) ;
    }

    @Override
    public String parseDeleteCql(TableToBeanConfig beanConfig , PgWalChange change ) {
        //match (f:Field{id:'${fieldId}' })-[r:SFMR { reId:'${reId}'  }]->(m:Module{moduleId:'${moduleId}'  }) delete r
        String  cql=this.getRawDeleteCql();
        Map<String,Object> valuesMap = new HashMap<String,Object>(2);
        return  parseDeleteDetailCql(  cql, beanConfig ,  change , valuesMap);
    }
}
