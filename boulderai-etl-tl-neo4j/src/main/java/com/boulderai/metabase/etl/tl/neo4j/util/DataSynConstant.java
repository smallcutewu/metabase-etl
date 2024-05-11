package com.boulderai.metabase.etl.tl.neo4j.util;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @ClassName: DataSynConstant
 * @Description: 工程常量定义
 * @author  df.l
 * @date 2022年09月05日
 * @Copyright boulderaitech.com
 */

public class DataSynConstant {

    public static final Long    ONE_HOUR_SECONDS=60L*60 *  2;
    public static final String    TABLE_ALL_PK_COLUMN_KEY="TABLE_ALL_PK_COLUMN_KEY";

    public static final String QUEUE="queue";

    /**
     * tc.table_schema= 'public'   and
     */
    public static final String SQL_QUERY_ALL_TABLE_PRIMARY_KEY=" select a.* from (  " +
            " SELECT DISTINCT  tc.constraint_type,     " +
            "               tc.constraint_name,      " +
            "                tc.table_name,     " +
            "                kcu.column_name,    " +
            "                tc.table_schema,      " +
            "               ccu.table_name AS foreign_table_name,     " +
            "               ccu.column_name AS foreign_column_name    " +
            "             FROM     " +
            "               information_schema.table_constraints AS tc      " +
            "               JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name     " +
            "               JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name     " +
            "             WHERE   tc.constraint_type = 'PRIMARY KEY'    " +
            "             and tc.table_schema in ('public','base','master_data','tp')  " +
            "        ) a join public.da_logic_entity  b  " +
            "         on a.table_name=b.table_name  ";

    public static final String SQL_QUERY_ALL_LOGIC_RELATION="     select   id   ,  name   , code   ,     " +
            "    description   ,     " +
            "    start_id as startId  ,     " +
            "    end_id  as endId ,     " +
            "    start_attr_id as startAttrId ,     " +
            "    end_attr_id  as endAttrId  ,     " +
            "    scene_id   as sceneId  ,     " +
            "    restriction   ,      " +
            "    namespace_id  as namespaceId ,     " +
            "    status   ,     " +
            "    scene_code   as sceneCode  from  da_logic_relation";

    /**
     * 需要同步的neo4j表配置
     */
    public static final String WAL_FILTER_TABLE_QUAERY_SQL = " select enable,table_name, app_id,db_type,  object_type,  relation_name, start_field_column,  end_field_column, ref_model_id,table_desc,created_at,updated_at,start_field_table,end_field_table," +
            "  start_field_ref_column ,end_field_ref_column ,no_syn_self_record  from  da_wal_sync_config   ";


    /**
     * tc.table_schema= 'public'   and
     */
    public static final String SQL_QUERY_TABLE_PRIMARY_KEY_BY_TABLE_NAME=" SELECT DISTINCT tc.constraint_type,     tc.constraint_name,   " +
            "    tc.table_name,      kcu.column_name,   " +
            "   ccu.table_name AS foreign_table_name,  " +
            "   ccu.column_name AS foreign_column_name " +
            " FROM  " +
            "   information_schema.table_constraints AS tc   " +
            "   JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name  " +
            "   JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name  " +
            " WHERE    tc.constraint_type = 'PRIMARY KEY'  and tc.table_name = ?  ";

    public static final String  IP_LOCAL_DEV_IP="172.16.10";

    public static final String SLOT_NAME_MATA_DEV = "metabase_slot_local";

    public static final String ZK_NODE_MASTER_NAME = "/nodes/metabase_local_dev/wal/master";

    public static final String DA_TABLE_TO_BEAN_CONFIG_SQL = " select  id,table_name as tableName ,class_name as className,status,     " +
            "            check_value as checkValue,check_column as checkColumn, bean_name as  beanName, syn_type as synType,    " +
            "  relation_name  as  relationName  ,    " +
            "  delete_column  as  deleteColumn ,    " +
            "   delete_column_value  as deleteColumnValue              " +
            "           from  da_table_to_bean_config where status= 1 ";

    public static final String DA_COLUMN_TO_FIELD_CONFIG_SQL = "     select   table_column as tableColumn" +
            ",bean_field  as beanField " +
            ",bean_name as beanName" +
            ",description " +
            " ,primary_key as  primaryKey      " +
            "    ,default_value  as  defaultValue      " +
            "    ,bean_field_type  as  beanFieldType      " +
            "  ,extend_column_sql  as extendColumnSql " +
            "  ,extend_sql_columns  as extendSqlColumns " +
            "  ,extend_sql_return_field  as extendSqlReturnField   "+
            " from  da_column_to_field_config      " +
            " where   status=1   and bean_name= ?  order by id  ";


    public static final String DA_ENTITY_ATTRIBUTE_SQL = " select  code from  da_entity_attribute where id=?  ";
 public static final String DA_LOGIC_ENTITY_CONFIG_SQL = " select     id  ,     name  ,    " +
            "   code  ,      alias  ,     description,     level  ,      parent_id  as parentId ,    " +
            "   namespace_id  as namespaceId,      status  ,       created_at as createdAt ,    " +
            "   updated_at as updatedAt ,      created_by  as createdBy,     updated_by as updatedBy ,    " +
            "   english_name  as englishName ,     is_main_entity as mainEntity  ,    table_name as tableName ,    " +
            "   data_type as dataType ,    " +
            "   namespace_code  as namespaceCode from  da_logic_entity where status >=2 " ;
//         " where status=2 ";

    public static final String DA_LOGIC_ENTITY_CONFIG_BY_TABLE_SQL = " select     id  ,     name  ,    " +
            "   code  ,      alias  ,     description,     level  ,      parent_id  as parentId ,    " +
            "   namespace_id  as namespaceId,      status  ,       created_at as createdAt ,    " +
            "   updated_at as updatedAt ,      created_by  as createdBy,     updated_by as updatedBy ,    " +
            "   english_name  as englishName ,     is_main_entity as mainEntity  ,    table_name as tableName ,    " +
            "   data_type as dataType ,    " +
            "   namespace_code  as namespaceCode from  da_logic_entity where table_name=? ";

    public static final String DA_LOGIC_ENTITY_CONFIG_BY_TABLE_ID_SQL = " select     id  ,     name  ,    " +
            "   code  ,      alias  ,     description,     level  ,      parent_id  as parentId ,    " +
            "   namespace_id  as namespaceId,      status  ,       created_at as createdAt ,    " +
            "   updated_at as updatedAt ,      created_by  as createdBy,     updated_by as updatedBy ,    " +
            "   english_name  as englishName ,     is_main_entity as mainEntity  ,    table_name as tableName ,    " +
            "   data_type as dataType ,    " +
            "   namespace_code  as namespaceCode from  da_logic_entity where id=? ";


    /**
     * tc.table_schema= 'public'
     */
    public static final String ALL_TABLE_PRIMARY_KEY_SQL =" SELECT    tc.table_name,   kcu.column_name  ,tc.table_schema  " +
            "       FROM        information_schema.table_constraints AS tc       " +
            "       JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name      " +
            "       JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name      " +
            "       WHERE    tc.constraint_type = 'PRIMARY KEY'  ORDER BY  tc.table_name  ";
    public static final String SCENE_BASE_INFO_SQL = " select  id from  scene_base_info where code=?  ";

    public final static String DATE_LONG_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public final static String DATE_SHORT_FORMAT = "yyyy-MM-dd";

    public final static String   MQ_EXCHANGE_VALUE="topic.metabase.wal4pg_local";
    public final static String   MQ_ROUTING_KEY="key.wal4pg_local";
    public final static String   MQ_QUEUE="queue.metabase.wal4pg_local";

//    public final static String   MQ_EXCHANGE_MODEL_DATA_VALUE="topic.model.data.wal4pg";
//    public final static String   MQ_ROUTING_MODEL_DATA_KEY="key.model.data.wal4metabase";
//    public final static String   MQ_MODEL_DATA_QUEUE="queue.model.data.wal4metabase";


    public final static String   MQ_EXCHANGE_OTHER_DATA_VALUE="topic.other.data.wal4pg";
    public final static String   MQ_ROUTING_OTHER_DATA_KEY="key.other.data.wal4pg";
    public final static String   MQ_OTHER_DATA_QUEUE="queue.other.data.wal4metabase";




    public final static String   MQ_EXCHANGE_INTEGRATION_DATA_VALUE="topic.integration.data.wal4pg";
    public final static String   MQ_ROUTING_INTEGRATION_DATA_KEY="key.integration.data.wal4pg";
    public final static String   MQ_INTEGRATION_DATA_QUEUE="queue.integration.data.wal4metabase";



    public final static String   MQ_EXCHANGE_NPI_DATA_VALUE="topic.npi.data.wal4pg";
    public final static String   MQ_ROUTING_NPI_DATA_KEY="key.npi.data.wal4pg";
    public final static String   MQ_NPI_DATA_QUEUE="queue.npi.data.wal4metabase";


    public final static String   MQ_EXCHANGE_EIMOS_DATA_VALUE="topic.eimos.data.wal4pg";
    public final static String   MQ_ROUTING_EIMOS_DATA_KEY="key.eimos.data.wal4pg";
    public final static String   MQ_EIMOS_DATA_QUEUE="queue.eimos.data.wal4metabase";



    public final static String   MQ_EXCHANGE_FAILMSG_DATA_VALUE="topic.failmsg.data.wal4pg";
    public final static String   MQ_ROUTING_FAILMSGI_DATA_KEY="key.failmsg.data.wal4pg";
    public final static String   MQ_FAILMSG_DATA_QUEUE="queue.failmsg.data.wal4metabase";
//    public final static String   MQ_FAILMSG_DATA_QUEUE_TEST="queue.failmsg.test.data.wal4metabase";
//    public final static String   MQ_ROUTING_FAILMSGI_DATA_KEY_TEST="key.failmsg.test.data.wal4pg";



    public final static String   MQ_EXCHANGE_OTHER_TEST_VALUE="topic.other.performance.test";
    public final static String   MQ_ROUTING_OTHER_TEST_KEY="key.other.performance.test";
    public final static String   MQ_OTHER_TEST_QUEUE="queue.other.performance.test";

    public final static String   MQ_EXCHANGE_INTEGRATION_CLUSTER_VALUE="topic.integration.data.cluster";
    public final static String   MQ_EXCHANGE_INTEGRATION_CLUSTER_KEY="key.integration.data.cluster";
    public final static String   MQ_INTEGRATION_CLUSTER_QUEUE="queue.integration.data.cluster";

    public final static String   MQ_EXCHANGE_MONITOR_DATA_VALUE="topic.monitor.data.wal4pg";
    public final static String   MQ_ROUTING_MONITOR_DATA_KEY="key.monitor.data.wal4pg";
    public final static String   MQ_MONITOR_DATA_QUEUE="queue.monitor.data.wal4metabase";


    public final static String  METABASE_WAL_SCHEMA="metabase_wal";

    /**
     * tc.table_schema= 'public'   and
     */
    public static final String SQL_QUERY_ALL_TABLE_PRIMARY_KEYS=" SELECT DISTINCT tc.constraint_type,  " +
            "   tc.constraint_name,   " +
            "    tc.table_name,  " +
            "    kcu.column_name,   " +
            "   ccu.table_name AS foreign_table_name,  " +
            "   ccu.column_name AS foreign_column_name " +
            " FROM  " +
            "   information_schema.table_constraints AS tc   " +
            "   JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name  " +
            "   JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name  " +
            " WHERE     tc.constraint_type = 'PRIMARY KEY'   ";


    public  static  final  String WAL_META_NPI_TABLE="npi";
    public  static  final  String WAL_META_QUEUE_NAME="wal_meta_queue";
    public  static  final  String WAL_MODEL_DATA_QUEUE_NAME="wal_model_data_queue";




    public  static  final  String WAL_INTEGRATION_MODEL_DATA_QUEUE_NAME="wal_integration_model_data_queue";
    public  static  final  String WAL_NPI_MODEL_DATA_QUEUE_NAME="wal_npi_model_data_queue";
    public  static  final  String WAL_EIMOS_MODEL_DATA_QUEUE_NAME="wal_eimos_model_data_queue";
    public  static  final  String WAL_OTHER_MODEL_DATA_QUEUE_NAME="wal_other_model_data_queue";

    public  static  final  String WAL_PERFORMANCE_TEST_QUEUE_NAME="wal_performance_queue";

    public  static  final  int TARGET_BUSSINESS_TYPE_360=0;
    public  static  final  int TARGET_BUSSINESS_TYPE_METABASE=1;
    public  static  final  int TARGET_BUSSINESS_TYPE_OTHER=2;
    public  static  final  int TARGET_BUSSINESS_TYPE_ALL=3;


    public static final String SQL_LOGIC_RELATION_SIMPLE_CONFIG=" select a.id, a.name   , a.code  , a.start_id as startId   , a.end_id as endId   , a.start_attr_id as startAttrId   , a.end_attr_id as endAttrId   , a.scene_id  as  sceneId ,   a.restriction   ,a.namespace_id  as  namespaceId   , a.status   , a. type   ,a.label_id as  labelId   , a.is_rer  as isRer    " +
            "  from public.da_logic_relation   a " +
            "  where     a.type in (1,7)  and ( a.start_id=? or a.end_id=? ) ";



    public static final String SQL_LOGIC_RELATION_MULTI_CONFIG=" SELECT a.id, a.name   , a.code  , a.start_id as startId   , a.end_id as endId   ,   a.attr_mapping  as   attrMapping ,    " +
            "               a.start_attr_id as startAttrId   , a.end_attr_id as endAttrId   , a.scene_id  as  sceneId ,   a.restriction   ,a.namespace_id  as  namespaceId   , a.status   , a. type   ,a.label_id as  labelId   , a.is_rer  as isRer             " +
            " FROM   da_logic_relation a  where    type in (1,7,8,9)   and (  start_id  ,  end_id  ) in (select   start_id  ,  end_id   from   da_logic_relation         " +
            " where    type in (1,7,8,9)  and (start_id=?  or end_id=?    )   " +
            " group by   start_id  ,  end_id   having count (  start_id  ) > 0  or   count (  end_id  ) >0   )  ORDER BY  a.start_id    , a.end_id ";



    public static final String SQL_TABLE_NAME_BY_ID=" select table_name  as tableName from  public.da_logic_entity  where id=? ";
    public static final String SQL_ATTR_NAME_BY_ID=" select code  as attName from  public.da_entity_attribute  where id=? ";

    public static final String DIS_ACTIVE_SLOT_SQL=" select  slot_name  from   pg_replication_slots  where   database=? and  active is  false and plugin = 'pgoutput'";
    public static final String DROP_SLOT_SQL=" select pg_drop_replication_slot('%s')  ";
    public static final String CREATE_SLOT_SQL=" select * from pg_create_logical_replication_slot('%s','pgoutput') ";
    public static final  Pattern JDBC_PATTERN = Pattern.compile("jdbc:postgresql://.*?/(metabase.*?)\\?");


    public static final String MAX_SLOT_ID_SQL=" select max(id) as maxId from  system_core.pg_slot_name_history ";
    public static final String INSERT_SLOT_SQL=" insert into   system_core.pg_slot_name_history ( slot_name )  values (?) ";
    public static final String SLOT_NAME_MODEL="metabase_slot_";

    public static final String ALL_TABLE_IN_SCHEMA=" SELECT tb.table_name,tb.table_schema  FROM information_schema.tables tb   " +
            "  JOIN pg_class c ON c.relname = tb.table_name  LEFT JOIN pg_description   " +
            "  d ON d.objoid = c.oid AND d.objsubid  " +
            " = '0'  WHERE tb.table_schema = ?  ";

    public static final String WAL_SUBSCRIBE_SCHEMA_LIST=  "public,base,master_data,tp,demo";

    public static final String LOGIC_RELATION_TABLE_INFO_SQL=  "   SELECT a.id,  a.start_id   ,    a.end_id  ,       " +
            "     (select table_name  from  da_logic_entity s where s.id=a.start_id) as start_table_name ,    " +
            "    (select table_name  from  da_logic_entity s where s.id=a.end_id) as end_table_name   ,    " +
            "      a.attr_mapping    " +
            "    FROM   da_logic_relation a  where    type in (1,7)   ";

    public static final String   CREATE_INDEX_NORMAL_CQL=" CREATE INDEX ON :${tableName}(${columnName})";
    public static final String   DROP_INDEX_NORMAL_CQL=" drop INDEX ON :${tableName}(${columnName})";

    public static final String   INSERT_CQL_ERROR_LOG_CQL=" INSERT INTO system_core.cql_error_log ( schema, table_name, error_type, cql, error_desc, wal_change, create_time) VALUES ( ?, ?, ?, ?, ?, ?, now()  )";

    public static final  String CREATE_CONSTRAINT_CQL="  CREATE CONSTRAINT  ${tableName}_pk   if not exists  ON   (n:${tableName}) ASSERT n.${pk} IS UNIQUE ";
    public static final String    DROP_CONSTRAINT_CQL ="  drop constraint        ON   (n:${tableName}) ASSERT n.${pk} IS UNIQUE ";
    public static final String    DROP_TABLE_DATA_CONSTRUCTOR_CQL ="  match (n:${tableName})  detach delete n ";


    public static final String SQL_QUERY_ALL_TABLE_MODULE=" select table_name from public.da_logic_entity order by id ";

    public static final String TABLE_PRIMARY_KEY_COLUMN_SQL="   SELECT DISTINCT b.column_name,a.table_schema,a.table_name FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS  a   " +
            "            LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE  b   " +
            "            ON a.constraint_name = b.constraint_name    " +
            "            WHERE   a.CONSTRAINT_TYPE ='PRIMARY KEY'  ";


    public static final String TABLE_all_COLUMN_SQL=
                    "                          select        DISTINCT    yyy.attname AS field,         " +
                    "                            yyy.typname AS type,       " +
                    "                            yyy.attlen AS length,        " +
                    "                        yyy.atttypmod  AS lengthvar,       " +
                    "                             yyy.attnotnull AS notnull,        " +
                    "                           yyy.description  AS comment ,    " +
                    "                       yyy.relname    as table_name  ，tb.table_schema from ( " +
            "                  SELECT    " +
            "                                    DISTINCT  " +
            "                                     a.attnum,      " +
            "                                      a.attname ,       " +
            "                                      t.typname ,       " +
            "                                      a.attlen ,       " +
            "                                      a.atttypmod ,     " +
            "                                      a.attnotnull,      " +
            "                                      b.description  ,  " +
            "                                       relname       " +
            "                                             " +
            "                                      FROM pg_class c,       " +
            "                                      pg_attribute a       " +
            "                                      LEFT OUTER JOIN pg_description b  " +
            "                                        ON a.attrelid=b.objoid AND a.attnum = b.objsubid,       " +
            "                                      pg_type t          WHERE   " +
            "                                      c.relname = ?      " +
            "                                      and a.attnum > 0       " +
            "                                      and a.attrelid = c.oid      " +
            "                                      and a.atttypid = t.oid      " +
            "                                      ORDER BY a.attnum   " +
            "                                      ) yyy left JOIN information_schema.tables tb    " +
            " ON yyy.relname = tb.table_name   and tb.table_schema =?   ";


    public final  static  Integer  ERTYPE_SIMPLE=1;

    public final  static  Integer  ERTYPE_MUTIL=2;

    public static final String   PG_WAL_CONN_EXCEPTION_ERROR= "connection failed when reading from copy";
    public static final String   PG_WAL_CONN_EXCEPTION_ERROR_2= "OutOfMemoryError";
    public static final String   PG_WAL_CONN_EXCEPTION_ERROR_5= "内存用尽";
    public static final String   PG_WAL_CONN_EXCEPTION_ERROR_3= "无法为包含";
    public static final String   PG_WAL_CONN_EXCEPTION_ERROR_4= "字节的字符串缓冲区扩大";

    public static final  Pattern  PATTERN=Pattern.compile("OutOfMemoryError|内存用尽|无法为包含|字节的字符串缓冲区扩大|Java heap space");

    public static final String   PG_SUB_TABLES_SQL= "select *  from  \"system_core\".\"pg_sub_tables\" where status=1 and  schema_name=? order by  sub_type ";

   public static final String   PG_CHECK_SUB_TABLES_SQL= "select *  from  \"system_core\".\"pg_sub_tables\" where status=1 and check_type=1 order by  query_order desc ";

    public static final String   CHECK_DATA_JOB_RES_SQL= "select   id,batch_id  as batchId,  schema_name  as schemaName, table_name as tableName, db_net_ok  as  dbNetOk,neo4j_net_ok    " +
            "    as  neo4jNetOk,    pg_count  as  pgCount,  neo4j_count  as  neo4jCount, check_result   as  checkResult,  create_time as createTime  from  \"system_core\".\"check_data_job_res\" where batch_id=?  ";

    public static final String   PG_REPLICATION_SLOTS_SQL= "select  count(1) as walCount  from   pg_replication_slots  where  database=?  and active is  true  and active_pid>0   and slot_name  like ? ";


    public static final String   PG_PATTER_TABLE_SQL= "  SELECT table_schema||'.'||tb.table_name as new_name, tb.table_name   FROM information_schema.tables tb       " +
            "                JOIN pg_class c ON c.relname = tb.table_name  LEFT JOIN pg_description        " +
            "                d ON d.objoid = c.oid AND d.objsubid       " +
            "               = '0'  WHERE tb.table_schema = ?  " +
            "               and tb.table_name   like ?  " +
            "               and tb.table_name not   like '%ver__backup' and tb.table_name not   like '%copy%'  ";


    
    public static final String   WAL_EXCLUDE_TABLE_LIST="isc_warehouse_period_snapshot,isc_bom_20230510,isc_bom_20230511";

    public static final String DEBEZIUMCHANGE_PYLOAD = "payload";

    public static final String DEBEZIUMCHANGE_BEFORE = "before";

    public static final String DEBEZIUMCHANGE_AFTER = "after";

    public static final String DEBEZIUMCHANGE_SCHEMA = "schema";

    public static final String DEBEZIUMCHANGE_FIELDS = "fields";

    public static final String DEBEZIUMCHANGE_READ = "\"op\":\"r\"";

    public static final String DEBEZIUMCHANGE_FIELD = "field";

    public static final String DEBEZIUMCHANGE_SOURCE = "source";

    public static final String DEBEZIUMCHANGE_TABLE = "table";

    public static final String DEBEZIUMCHANGE_OP = "op";

    public static final String DEBEZIUMCHANGE_TYPE = "type";

    public static final String DEBEZIUMCHANGE_NAME = "name";

    public static final String DEBEZIUMCHANGE_MICROTIMESTAMP = "io.debezium.time.MicroTimestamp";

    public static final String DEBEZIUMCHANGE_MICROTIME = "io.debezium.time.MicroTime";

    public static final String DEBEZIUMCHANGE_HEARTBEAT = "io.debezium.connector.common.Heartbeat";

    public static final String SYSTEM_NAME= "metabase-sync-starter";

    // 元数据相关
    public static String [] METATABLES = new String[] {"da_biz_object","da_entity_attribute","da_logic_relation","da_scene_biz_logic_relation","da_logic_entity","scene_base_info","da_scene_attr_logic_relation"};

    // 元数据相关集合
    public static List<String> METATABLELIST = Arrays.asList(METATABLES);
}
