package com.boulderai.metabase.etl.tl.neo4j.service.parser;

import com.boulderai.metabase.lang.er.RelationType;
import com.boulderai.metabase.etl.tl.neo4j.config.TableToBeanConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;

/**
 * @ClassName: RelationCqlParser
 * @Description: 关系cql解析器定义
 * @author  df.l
 * @date 2022年10月12日
 * @Copyright boulderaitech.com
 */
public interface RelationCqlParser {
    public  String parseInsertCql(TableToBeanConfig beanConfig , PgWalChange change );
    public  String parseUpdateCql(TableToBeanConfig beanConfig , PgWalChange change );
    public  String parseDeleteCql(TableToBeanConfig beanConfig , PgWalChange change );

    public  String parseDeleteModelDataCql(TableToBeanConfig beanConfig , PgWalChange change );
    public  Boolean needParseModelData();
    public  Boolean deleteOnUpsert();


    public  String getRawInsertCql();
    public  String getRawUpdateCql();
    public  String getRawDeleteCql();

    public RelationType getRelationType();

    ///DER
    public final static String RELATION_UPSERT_CQL=" match (left) -[r:DER {re_id:'${reId}'} ] -> ( right )  delete r  ";


    ////ER
    public  static  final String  ER_INSERT_CQL= " match (m:Module { module_id:'${sModuleId}'  }),(m1:Module {module_id:'${eModuleId}' }) merge (m)-[r:${erLabel}   {re_id:'${reId}' }]->(m1)  set r.from='${fromField}',r.to='${toField}',r.type='${restriction}' , r.scene_code='${scene_code}' ,r.label='${label}', r.er_label='${erLabel}' , r.is_rer='${is_rer}',r.attr_mapping='${attr_mapping}' ";
    public  static  final String  ER_DELETE_CQL= " match (m:Module ) - [r {  re_id:'${reId}'  }]->(m1:Module) delete r  ";
    public  static  final String  RELATION_KEY_ER="ER";

    /////    FMR("FMR", "属性 -> 逻辑实体"),
    public  static  final String  RELATION_KEY_FER="FER";
    public  static  final String  FER_INSERT_CQL=" match  (f:Field {id: '${fieldId}' }),  (m:Module {module_id:'${moduleId}' }) merge (f)-[r:FMR {  re_id:'${reId}' } ]->(m) set  r.options='${options}' ";
    public  static  final String  FER_DELETE_CQL=" match ( f:Field { id: '${fieldId}' }) -[r:FMR { re_id:'${reId}' }]->  ( m:Module ) delete r  ";


    ///////////################  SER
//
//    public  static  final String  SER_INSERT_CQL= " match (m:Module {module_id:'${sModuleId}' }),(m1:Module {module_id:'${eModuleId}' }) merge (m)-[r:SER {scene_key:'${sceneKey}',from:'${fromField}' ,to:'${toField}',type:'${erType}',re_id:'${reId}'  } ]->(m1) ";
//    public  static  final String  SER_DELETE_CQL= " match (m:Module )-[r:SER { re_id:'${reId}'  } ]-(m1:Module )  delete r ";
//    public  static  final String  RELATION_KEY_SER="SER";

    ///###############  SBR

    public  static  final String  SBR_INSERT_CQL= " match (b:Bo),(s:Scene) where s.scene_key='${sceneKey}' and b.code = '${biz_code}' MERGE (b)-[r:SBR {re_id:'${reId}'  } ]->(s) set r.scene_key='${sceneKey}' ";
   public  static  final String  SBR_DELETE_CQL= " match (b:Bo)-[r:SBR]->(s:Scene) where r.re_id='${reId}'  delete r  ";
    public  static  final String  RELATION_KEY_SBR="SBR";
    //###########SMBR

    public  static  final String  SMBR_INSERT_CQL= " match (b:Bo {id:'${boId}' }),(m:Module {module_id:'${moduleId}' }) MERGE (m)-[r:SMBR {re_id:'${reId}' }]->(b) set  r.scene_key= '${sceneKey}' ";
    public  static  final String  SMBR_DELETE_CQL= " match (m:Module )-[r:SMBR {re_id:'${reId}' }]->(b:Bo ) delete r  ";
    public  static  final String  RELATION_KEY_SMBR="SMBR";
    ////##########SFMR

    public  static  final String  SFMR_INSERT_CQL= " match (m:Module{module_id:'${moduleId}' }),(f:Field{id:'${fieldId}' }) MERGE (f)-[r:SFMR {re_id:'${reId}' }]->(m) set r.scene_key='${sceneKey}'  ";
    public  static  final String  SFMR_DELETE_CQL= " match (f:Field{id:'${fieldId}' })-[r:SFMR { re_id:'${reId}'  }]->(m:Module{module_id:'${moduleId}'  }) delete r  ";
    public  static  final String  RELATION_KEY_SFMR="SFMR";

    //////##########  MBOR
    public  static  final String  MBR_INSERT_CQL= " match ( m:Module{module_id:'${moduleId}' } ),(b:Bo {id:'${boId}' }) MERGE (m)-[ r:MBOR{  re_id:'${reId}' } ]->(b)  ";
    public  static  final String  MBR_DELETE_CQL= " match ( m:Module } )-[r:MBOR {  re_id:'${reId}' }]->(b:Bo ) delete r ";
    public  static  final String  RELATION_KEY_MBR="MBOR";
}
