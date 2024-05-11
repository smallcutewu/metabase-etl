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
public class SmbrRelationCqlParserTest  extends SpringBaseTest {

    @Test
    public void testParseInsertCql()
    {
        Gson gson = new Gson();
        String metaData=" {\"changeId\":\"da_scene_biz_logic_relation_75\",\"kind\":\"update\",\"schema\":\"public\",\"table\":\"da_scene_biz_logic_relation\",\"columnnames\":[\"id\",\"logic_code\",\"biz_id\",\"created_at\",\"description\",\"biz_code\",\"created_by\",\"updated_at\",\"updated_by\",\"scene_code\",\"status\",\"logic_id\",\"biz_name\",\"logic_name\"],\"columntypes\":[\"bigint\",\"character varying(100)\",\"bigint\",\"timestamp without time zone\",\"character varying(1000)\",\"character varying(1000)\",\"character varying(1000)\",\"timestamp without time zone\",\"character varying(1000)\",\"character varying(50)\",\"integer\",\"bigint\",\"character varying(200)\",\"character varying(200)\"],\"columnvalues\":[\"1368\",\"ltc_account_contact\",\"3930290149\",\"2022-11-09 19:41:01\",null,\"ltc_marketing_account\",null,null,null,\"ismp_01\",null,\"299082902669361667\",\"营销客户\",\"ISMP_客户管理_营销客户联系人\"],\"oldkeys\":{\"keynames\":[\"id\"],\"keytypes\":[\"bigint\"],\"keyvalues\":[\"1368\"]}} ";
        PgWalChange record = gson.fromJson(metaData, PgWalChange.class);
        String tableName = "da_scene_biz_logic_relation";
        Neo4jSyncContext.getInstance().initData();
        List<TableToBeanConfig> tableToBeanConfigs= Neo4jSyncContext.getInstance().getTableToBeanRelationConfigByTableName(tableName);
        TableToBeanConfig tableToBeanConfig= tableToBeanConfigs.get(0);
        SmbrRelationCqlParser  parser=new SmbrRelationCqlParser();
        String cql=parser.parseInsertCql(tableToBeanConfig,record);
        Assert.assertNotNull(cql);

        cql=parser.parseUpdateCql(tableToBeanConfig,record);
        Assert.assertNotNull(cql);

        cql=parser.parseDeleteCql(tableToBeanConfig,record);
        Assert.assertNotNull(cql);

    }

}