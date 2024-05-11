package com.boulderai.metabase.etl.tl.neo4j.service.parser;

import com.boulderai.metabase.etl.tl.neo4j.config.TableToBeanConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.Column;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import org.apache.commons.text.StringSubstitutor;

import java.util.Map;

/**
 * @ClassName: BaseRelationCqlParser
 * @Description: 关系cql解析适配类
 * @author  df.l
 * @date 2022年10月12日
 * @Copyright boulderaitech.com
 */
public abstract class BaseRelationCqlParser implements  RelationCqlParser{
    @Override
    public String parseInsertCql(TableToBeanConfig beanConfig , PgWalChange change ) {
        return null;
    }

    @Override
    public String parseUpdateCql(TableToBeanConfig beanConfig , PgWalChange change ) {
        return null;
    }

    @Override
    public String parseDeleteCql(TableToBeanConfig beanConfig , PgWalChange change ) {
        return null;
    }

    @Override
    public  String parseDeleteModelDataCql(TableToBeanConfig beanConfig , PgWalChange change )
    {
        return null;
    }

    @Override
    public  Boolean needParseModelData()
    {
        return false;
    }

    @Override
    public  Boolean deleteOnUpsert(){
        return false;
    }



    protected  void setColumnValue2Map(String columnName, String valueKey, PgWalChange change , Map<String,Object> valuesMap, Boolean useString,Boolean replaceFlag)
    {
        Column sceneIdColumn=  change.makeOneColumn(columnName);
        if(sceneIdColumn!=null)
        {
            Object  sceneIdValue=sceneIdColumn.getRealValue();
            if(sceneIdValue!=null)
            {
                if(useString)
                {
                    if (replaceFlag) {

                        valuesMap.put(valueKey,String.valueOf(sceneIdValue). replace("'", "\\'"));
                    }
                    else {
                        valuesMap.put(valueKey,String.valueOf(sceneIdValue));
                    }

                }
                else {
                    valuesMap.put(valueKey,sceneIdValue);
                }
            }
            else
            {
                valuesMap.put(valueKey,"");
            }
        }
        else {
            if(useString)
            {
                valuesMap.put(valueKey,"");
            }
            else
            {
                valuesMap.put(valueKey,null);
            }
        }
    }

    protected  String mergeCql(String cql,Map<String,Object> valuesMap)
    {
        StringSubstitutor sub = new StringSubstitutor(valuesMap);
        String  resCql= sub.replace(cql);
        valuesMap.clear();
        return resCql;
    }

    protected String  parseDeleteDetailCql(String  cql,TableToBeanConfig beanConfig , PgWalChange change ,Map<String,Object> valuesMap)
    {
        setColumnValue2Map( "id","reId", change , valuesMap,true,false);
        StringSubstitutor sub = new StringSubstitutor(valuesMap);
        String  resCql= sub.replace(cql);
        valuesMap.clear();
        return resCql;
    }


}
