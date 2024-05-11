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
public class FerRelationCqlParserTest  extends SpringBaseTest {

    @Test
    public void testParseInsertCql()
    {
        Gson gson = new Gson();
        String metaData="  {\"changeId\":\"da_logic_relation_4\",\"kind\":\"update\",\"schema\":\"public\",\"table\":\"da_logic_relation\",\"columnnames\":[\"id\",\"name\",\"code\",\"description\",\"start_id\",\"end_id\",\"start_attr_id\",\"end_attr_id\",\"scene_id\",\"restriction\",\"tags\",\"namespace_id\",\"status\",\"created_at\",\"updated_at\",\"created_by\",\"updated_by\",\"scene_code\",\"label\",\"type\"],\"columntypes\":[\"bigint\",\"character varying(100)\",\"character varying(100)\",\"character varying(4000)\",\"bigint\",\"bigint\",\"bigint\",\"bigint\",\"bigint\",\"character varying(4000)\",\"text[]\",\"bigint\",\"integer\",\"timestamp without time zone\",\"timestamp without time zone\",\"character varying(100)\",\"character varying(100)\",\"text[]\",\"character varying(200)\",\"smallint\"],\"columnvalues\":[\"1039012182778015744\",\"test\",\"test\",null,\"255881161685861400\",\"258235862540617020\",\"255881161686058008\",\"258235862540748092\",null,\"1:N\",null,\"255450917715837976\",\"1\",\"2022-11-07 11:03:28.287\",null,\"熊强\",null,null,null,\"1\"],\"oldkeys\":{\"keynames\":[\"id\"],\"keytypes\":[\"bigint\"],\"keyvalues\":[\"1039012182778015744\"]}} ";
        PgWalChange record = gson.fromJson(metaData, PgWalChange.class);
        String tableName = "da_logic_relation";
        Neo4jSyncContext.getInstance().initData();
        List<TableToBeanConfig> tableToBeanConfigs= Neo4jSyncContext.getInstance().getTableToBeanRelationConfigByTableName(tableName);
        TableToBeanConfig tableToBeanConfig= tableToBeanConfigs.get(0);
        FerRelationCqlParser  parser=new FerRelationCqlParser();
        String cql=parser.parseInsertCql(tableToBeanConfig,record);
        Assert.assertNotNull(cql);

        cql=parser.parseUpdateCql(tableToBeanConfig,record);
        Assert.assertNotNull(cql);

        cql=parser.parseDeleteCql(tableToBeanConfig,record);
        Assert.assertNotNull(cql);

    }
}
