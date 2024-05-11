package com.boulderai.metabase.etl.tl.neo4j.service.parser;

import com.boulderai.metabase.etl.tl.neo4j.config.TableToBeanConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.Neo4jSyncContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
@RunWith(MockitoJUnitRunner.Silent.class)
public class SfmrRelationCqlParserTest  extends SpringBaseTest {

    @Test
    public void testParseInsertCql()
    {
        Gson gson = new Gson();
        String metaData=" {\"changeId\":\"da_scene_attr_logic_relation_68\",\"kind\":\"update\",\"schema\":\"public\",\"table\":\"da_scene_attr_logic_relation\",\"columnnames\":[\"id\",\"description\",\"logic_id\",\"logic_code\",\"attr_id\",\"attr_code\",\"scene_code\",\"status\",\"create_at\",\"create_by\",\"update_by\",\"update_at\",\"owned_org_id\",\"owned_by\",\"created_by\",\"created_at\",\"updated_by\",\"updated_at\",\"deleted_by\",\"deleted_at\",\"scene_id\"],\"columntypes\":[\"bigint\",\"character varying(1000)\",\"bigint\",\"character varying(100)\",\"bigint\",\"character varying(100)\",\"character varying(50)\",\"integer\",\"timestamp without time zone\",\"character varying(100)\",\"character varying(100)\",\"timestamp without time zone\",\"character varying(50)\",\"character varying(50)\",\"character varying(50)\",\"timestamp without time zone\",\"character varying(50)\",\"timestamp without time zone\",\"character varying(50)\",\"timestamp without time zone\",\"bigint\"],\"columnvalues\":[\"736\",null,\"255881162071737368\",\"合同回款信息\",\"255881162072196120\",\"contract_no\",\"tp_contract_scene\",\"1\",\"2022-11-09 19:50:11\",null,null,null,null,null,null,null,null,null,null,null,null],\"oldkeys\":{\"keynames\":[\"id\"],\"keytypes\":[\"bigint\"],\"keyvalues\":[\"736\"]}} ";
        PgWalChange record = gson.fromJson(metaData, PgWalChange.class);
        String tableName = "da_scene_attr_logic_relation";
        Neo4jSyncContext.getInstance().initData();
        List<TableToBeanConfig> tableToBeanConfigs= Neo4jSyncContext.getInstance().getTableToBeanRelationConfigByTableName(tableName);
        TableToBeanConfig tableToBeanConfig= tableToBeanConfigs.get(0);
        SfmrRelationCqlParser  parser=new SfmrRelationCqlParser();
        String cql=parser.parseInsertCql(tableToBeanConfig,record);
        Assert.assertNotNull(cql);

        cql=parser.parseUpdateCql(tableToBeanConfig,record);
        Assert.assertNotNull(cql);

        cql=parser.parseDeleteCql(tableToBeanConfig,record);
        Assert.assertNotNull(cql);

    }

}