package com.boulderai.metabase.etl.tl.neo4j;

import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.SyncPostgresConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.ZookeeperConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.Column;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWal2JsonRecord;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: BaseTest
 * @Description: 单元测试基础类
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public class BaseTest {

    protected Neo4jConfig createNeo4jConfig()
    {
        Neo4jConfig  config=new Neo4jConfig();
        config.setUsername("neo4j");

//        config.setAutoIndex("update");
//        config.setUri("bolt://127.0.0.1:7687");
//        config.setPassword("123456");

        config.setUri("bolt://172.16.5.62:7687");
        config.setPassword("Zzh!@7465671");
        return   config ;
    }


    protected SyncPostgresConfig createPostgresConfig()
    {
        SyncPostgresConfig config=new SyncPostgresConfig();
        config.setUrl("jdbc:postgresql://172.16.5.66:5432/metabase_unit_test");
        config.setUsername("postgres");
        config.setPassword("Zzh!@7465671");
        config.setDriverClassName("org.postgresql.Driver");
        config.setDbName("metabase");
        config.setUrl("jdbc:postgresql://172.16.5.66:5432/metabase_unit_test?currentSchema=public");
        return config;
    }

    public List<Column> makeColumnList()
    {
      List<Column>  columns=new ArrayList<Column>();
        for(int i=0;i<5;i++)
        {
            String name="column"+i;
            String type="text";
            String value="value"+i;
            Column  column=new Column( name,  type,  value);
            columns.add(column);
        }
        return columns;

    }


    protected PgWal2JsonRecord readUpdateWalJson()
    {
        String  walJson=" {\"changeId\":\"da_logic_entity_16\",\"kind\":\"update\",\"schema\":\"public\",\"table\":\"da_logic_entity\",\"columnnames\":[\"id\",\"name\",\"code\",\"alias\",\"description\",\"level\",\"parent_id\",\"owners\",\"tags\",\"namespace_id\",\"status\",\"created_at\",\"updated_at\",\"created_by\",\"updated_by\",\"english_name\",\"is_main_entity\",\"table_name\",\"data_type\",\"namespace_code\",\"table_type\",\"has_version\",\"options\"],\"columntypes\":[\"bigint\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"character varying(4000)\",\"smallint\",\"bigint\",\"text[]\",\"text[]\",\"bigint\",\"integer\",\"timestamp without time zone\",\"timestamp without time zone\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"boolean\",\"character varying(500)\",\"character varying(100)\",\"character varying(100)\",\"smallint\",\"smallint\",\"jsonb\"],\"columnvalues\":[\"279517896797849212\",\"新产品开发-外观设计申请\",\"ltc_np_face_design\",null,\"107数据导入\",\"6\",\"270247456799851414\",null,null,\"1039811320758874112\",\"2\",null,\"2022-11-23 14:31:04\",null,\"00138\",\"ltc_np_face_design\",\"false\",\"ltc_np_face_design\",null,\"master_data\",\"1\",\"0\",null],\"oldkeys\":{\"keynames\":[\"id\"],\"keytypes\":[\"bigint\"],\"keyvalues\":[\"279517896797849212\"]}} ";
        Gson gson = new Gson();
        PgWal2JsonRecord record = gson.fromJson(walJson, PgWal2JsonRecord.class);
        return record;
    }

    protected PgWal2JsonRecord readUpdateBadScenceJson()
    {
        String  walJson=
                "{\"nextlsn\":\"7C/990CBE70\",\"change\":[{\n" +
                        "\"schema\":\"public\",\n" +
                        "\"table\":\"scene_base_info\",\n" +
                        "\"columnnames\":[\n" +
                        "\"id\",\n" +
                        "\"name\",\n" +
                        "\"code\",\n" +
                        "\"description\",\n" +
                        "\"created_by\",\n" +
                        "\"created_at\",\n" +
                        "\"updated_by\",\n" +
                        "\"updated_at\",\n" +
                        "\"deleted_by\",\n" +
                        "\"deleted_at\",\n" +
                        "\"owner_org_code\",\n" +
                        "\"corp_id\",\n" +
                        "\"parent_code\",\n" +
                        "\"system_event\",\n" +
                        "\"data_source\",\n" +
                        "\"is_deleted\"\n" +
                        "],\n" +
                        "\"columntypes\":[\n" +
                        "\"bigint\",\n" +
                        "\"character varying(50)\",\n" +
                        "\"character varying(50)\",\n" +
                        "\"text\",\n" +
                        "\"character varying(255)\",\n" +
                        "\"timestamp without time zone\",\n" +
                        "\"character varying(255)\",\n" +
                        "\"timestamp without time zone\",\n" +
                        "\"character varying(255)\",\n" +
                        "\"timestamp without time zone\",\n" +
                        "\"character varying(255)\",\n" +
                        "\"character varying(255)\",\n" +
                        "\"character varying(100)\",\n" +
                        "\"character varying(255)\",\n" +
                        "\"character varying(255)\",\n" +
                        "\"boolean\"\n" +
                        "],\n" +
                        "\"columnvalues\":[\n" +
                        "556,\n" +
                        "\"产品\",\n" +
                        "\"mdm_product_grap\",\n" +
                        "null,\n" +
                        "\"00113\",\n" +
                        "\"2022-12-09 09:03:54.064\",\n" +
                        "\"81\",\n" +
                        "\"2023-01-09 10:27:02.431372\",\n" +
                        "null,\n" +
                        "null,\n" +
                        "null,\n" +
                        "null,\n" +
                        "\"mdm_product_base\",\n" +
                        "\"\",\n" +
                        "null,\n" +
                        "false\n" +
                        "],\n" +
                        "\"oldkeys\":{\n" +
                        "\"keynames\":[\n" +
                        "\"id\"\n" +
                        "],\n" +
                        "\"keytypes\":[\n" +
                        "\"bigint\"\n" +
                        "],\n" +
                        "\"keyvalues\":[\n" +
                        "556\n" +
                        "]\n" +
                        "}\n" +
                        "}]\n" +
                        "} ";

        Gson gson = new Gson();
        PgWal2JsonRecord record = gson.fromJson(walJson, PgWal2JsonRecord.class);
        return record;
    }

    protected PgWal2JsonRecord readUpdateScenceJson()
    {
        String  walJson=
                "{\"nextlsn\":\"7C/990CBE70\",\"change\":[{\n" +
                "\"kind\":\"update\",\n" +
                "\"schema\":\"public\",\n" +
                "\"table\":\"scene_base_info\",\n" +
                "\"columnnames\":[\n" +
                "\"id\",\n" +
                "\"name\",\n" +
                "\"code\",\n" +
                "\"description\",\n" +
                "\"created_by\",\n" +
                "\"created_at\",\n" +
                "\"updated_by\",\n" +
                "\"updated_at\",\n" +
                "\"deleted_by\",\n" +
                "\"deleted_at\",\n" +
                "\"owner_org_code\",\n" +
                "\"corp_id\",\n" +
                "\"parent_code\",\n" +
                "\"system_event\",\n" +
                "\"data_source\",\n" +
                "\"is_deleted\"\n" +
                "],\n" +
                "\"columntypes\":[\n" +
                "\"bigint\",\n" +
                "\"character varying(50)\",\n" +
                "\"character varying(50)\",\n" +
                "\"text\",\n" +
                "\"character varying(255)\",\n" +
                "\"timestamp without time zone\",\n" +
                "\"character varying(255)\",\n" +
                "\"timestamp without time zone\",\n" +
                "\"character varying(255)\",\n" +
                "\"timestamp without time zone\",\n" +
                "\"character varying(255)\",\n" +
                "\"character varying(255)\",\n" +
                "\"character varying(100)\",\n" +
                "\"character varying(255)\",\n" +
                "\"character varying(255)\",\n" +
                "\"boolean\"\n" +
                "],\n" +
                "\"columnvalues\":[\n" +
                "556,\n" +
                "\"产品\",\n" +
                "\"mdm_product_grap\",\n" +
                "null,\n" +
                "\"00113\",\n" +
                "\"2022-12-09 09:03:54.064\",\n" +
                "\"81\",\n" +
                "\"2023-01-09 10:27:02.431372\",\n" +
                "null,\n" +
                "null,\n" +
                "null,\n" +
                "null,\n" +
                "\"mdm_product_base\",\n" +
                "\"\",\n" +
                "null,\n" +
                "false\n" +
                "],\n" +
                "\"oldkeys\":{\n" +
                "\"keynames\":[\n" +
                "\"id\"\n" +
                "],\n" +
                "\"keytypes\":[\n" +
                "\"bigint\"\n" +
                "],\n" +
                "\"keyvalues\":[\n" +
                "556\n" +
                "]\n" +
                "}\n" +
                "}]\n" +
                "} ";

        Gson gson = new Gson();
        PgWal2JsonRecord record = gson.fromJson(walJson, PgWal2JsonRecord.class);
        return record;
    }

    protected   PgWal2JsonRecord readInsertWalJson()
    {
        String  walJson=" {\n" +
                "\t\"xid\": 12583810,\n" +
                "\t\"nextlsn\": \"1B/15C9BFD0\",\n" +
                "\t\"change\": [{\n" +
                "\t\t\"kind\": \"insert\",\n" +
                "\t\t\"schema\": \"public\",\n" +
                "\t\t\"table\": \"pg_wal_test\",\n" +
                "\t\t\"columnnames\": [\"id\", \"name\", \"price\", \"closed\", \"quality\", \"test\"],\n" +
                "\t\t\"columntypes\": [\"bigint\", \"character varying(255)\", \"numeric(10,2)\", \"boolean\", \"bigint\", \"smallint\"],\n" +
                "\t\t\"columnvalues\": [5, \"ggg\", 56.80, false, 21, 5]\n" +
                "\t}]\n" +
                "}";

        Gson gson = new Gson();
        PgWal2JsonRecord record = gson.fromJson(walJson, PgWal2JsonRecord.class);
        return record;
    }



    protected   PgWal2JsonRecord readDeleteWalJson()
    {
        String  walJson="{ \n" +
                "\t\"xid\": 12583814,\n" +
                "\t\"nextlsn\": \"1B/15C9EF08\",\n" +
                "\t\"change\": [{\n" +
                "\t\t\"kind\": \"delete\",\n" +
                "\t\t\"schema\": \"public\",\n" +
                "\t\t\"table\": \"pg_wal_test\",\n" +
                "\t\t\"oldkeys\": {\n" +
                "\t\t\t\"keynames\": [\"id\"],\n" +
                "\t\t\t\"keytypes\": [\"bigint\"],\n" +
                "\t\t\t\"keyvalues\": [4]\n" +
                "\t\t}\n" +
                "\t}]\n" +
                "} ";

        Gson gson = new Gson();
        PgWal2JsonRecord record = gson.fromJson(walJson, PgWal2JsonRecord.class);
        return record;
    }


    protected ZookeeperConfig createZookeeperConfig()
    {
        ZookeeperConfig  config=new ZookeeperConfig();
        config.setZookeeperUrl("172.16.5.48:2181");
        config.setMasterNodeName("/nodetest/test1");
        return   config ;
    }







}
