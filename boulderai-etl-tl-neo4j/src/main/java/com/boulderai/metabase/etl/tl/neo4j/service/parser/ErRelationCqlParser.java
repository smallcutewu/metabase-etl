package com.boulderai.metabase.etl.tl.neo4j.service.parser;

import com.boulderai.metabase.lang.er.ErType;
import com.boulderai.metabase.lang.er.RelationType;
import com.boulderai.metabase.etl.tl.neo4j.config.TableToBeanConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.Column;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.etl.tl.neo4j.util.DbOperateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;

import java.util.HashMap;
import java.util.Map;


/**
 * @ClassName: ErRelationCqlParser
 * @Description: ER关系关系cql解析适配类
 * @author  df.l
 * @date 2022年10月12日
 * @Copyright boulderaitech.com
 */
public class ErRelationCqlParser extends   BaseRelationCqlParser{

    public  static final String ER_LABEL_DEFAULT="ER";
    public  static final String ER_LABEL_RER="RER";
    public  static final Integer ER_LABEL_DEFAULT_VALUE=1;

    @Override
    public String getRawInsertCql() {
        return ER_INSERT_CQL;
    }

    @Override
    public String getRawUpdateCql() {
        return ER_INSERT_CQL;
    }

    @Override
    public String getRawDeleteCql() {
        return ER_DELETE_CQL;
    }

    @Override
    public RelationType getRelationType() {
        return RelationType.ER;
    }

    @Override
    public  Boolean needParseModelData()
    {
        return true;
    }

    @Override
    public  Boolean deleteOnUpsert(){
        return true;
    }

    @Override
    public  String parseDeleteModelDataCql(TableToBeanConfig beanConfig , PgWalChange change )
    {
        Map<String,Object> valuesMap = new HashMap<String,Object>(6);
        setColumnValue2Map( "id","reId", change , valuesMap,true,false);
        setColumnValue2Map( "label_id","label", change , valuesMap,true,false);

        StringSubstitutor sub = new StringSubstitutor(valuesMap);
        String  resCql= sub.replace(RELATION_UPSERT_CQL);
        valuesMap.clear();
        return resCql;
    }


    @Override
    public String parseInsertCql(TableToBeanConfig beanConfig , PgWalChange change ) {
        //match (m:Module { moduleId:${sModuleId}}),(m1 {moduleId:${eModuleId} }) merge (m)-[r:ER {from:${fromField},to:${toField},type:${erType}}]->(m1)
        //match (m:Module { moduleId:'${sModuleId}'  }),(m1 {moduleId:'${eModuleId}' }) merge (m)-[r:ER {from:'${fromField}',to:'${toField}',type:'${erType}' ,reId:'${reId}', scene_code:'${scene_code}' }]->(m1)

        String erLabel=null;
        Column typeColumn=  change.makeOneColumn("type");
        if(typeColumn!=null)
        {
            Object  typeValue=typeColumn.getRealValue();
            if(typeValue!=null)
            {
                ErType  erType= ErType.intToType((Integer) typeValue);
                if(erType.getType()>0)
                {
                    erLabel=erType.getDesc();
                }
            }
        }

        if(StringUtils.isBlank(erLabel))
        {
            return null;
        }

        String  cql=this.getRawInsertCql();
        Map<String,Object> valuesMap = new HashMap<String,Object>(6);
        addStartEndFieldInfo(  cql, beanConfig ,  change , valuesMap);
        valuesMap.put("erLabel",erLabel);
        return  parseDetailCql(  cql, beanConfig ,  change ,valuesMap);
    }

    @Override
    public String parseUpdateCql(TableToBeanConfig beanConfig , PgWalChange change ) {
        return  parseInsertCql(  beanConfig ,  change );
    }

    @Override
    public String parseDeleteCql(TableToBeanConfig beanConfig , PgWalChange change ) {
        String  cql=  getRawDeleteCql( );
        Map<String,Object> valuesMap = new HashMap<String,Object>(6);
//        addStartEndFieldInfo(  cql, beanConfig ,  change , valuesMap);
        return  parseDeleteDetailCql(  cql, beanConfig ,  change ,valuesMap);
    }

    private   void addStartEndFieldInfo(String  cql,TableToBeanConfig beanConfig , PgWalChange change ,Map<String,Object> valuesMap)
    {
        setColumnValue2Map( "restriction","restriction", change , valuesMap,true,true);
        setColumnValue2Map("attr_mapping", "attr_mapping", change, valuesMap, true,true);
        Column startColumn=  change.makeOneColumn("start_attr_id");
        Column endColumn=  change.makeOneColumn("end_attr_id");

        if(startColumn==null||endColumn==null)
        {
            return ;
        }
        Object  startFieldValue=startColumn.getRealValue();
        Object  endFieldValue=endColumn.getRealValue();
        if(startFieldValue==null||endFieldValue==null)
        {
            return ;
        }

        Map<String, Object> startFieldMap = DbOperateUtil.queryMap(DataSynConstant.DA_ENTITY_ATTRIBUTE_SQL,startFieldValue);
       if(startFieldMap!=null&&!startFieldMap.isEmpty())
       {
           String code= (String) startFieldMap.getOrDefault("code","");
           valuesMap.put("fromField",code);

       }
       else
       {
           valuesMap.put("fromField","");
       }

        Map<String, Object> endFieldMap = DbOperateUtil.queryMap(DataSynConstant.DA_ENTITY_ATTRIBUTE_SQL,endFieldValue);

        if(endFieldMap!=null&&!endFieldMap.isEmpty())
        {
            String code= (String) endFieldMap.getOrDefault("code","");
            valuesMap.put("toField",code);
        }
        else {
            valuesMap.put("toField","");
        }
    }

    protected String  parseDetailCql(String  cql,TableToBeanConfig beanConfig , PgWalChange change ,Map<String,Object> valuesMap)
    {
        setColumnValue2Map( "start_id","sModuleId", change , valuesMap,true,false);
        setColumnValue2Map( "end_id","eModuleId", change , valuesMap,true,false);
        setColumnValue2Map( "id","reId", change , valuesMap,true,false);
        setColumnValue2Map( "scene_code","scene_code", change , valuesMap,true,false);
        setColumnValue2Map( "label_id","label", change , valuesMap,true,false);
        setColumnValue2Map( "is_rer","is_rer", change , valuesMap,true,false);
        StringSubstitutor sub = new StringSubstitutor(valuesMap);
        String  resCql= sub.replace(cql);
        valuesMap.clear();
        return resCql;
    }


}
