package com.boulderai.metabase.etl.tl.neo4j.service.pipeline;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;
import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import com.google.gson.Gson;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @ClassName: MqMetabase2Neo4jProcessPipelineTest
 * @Description: 元数据pipeline类
 * @author  df.l
 * @date 2022年09月07日
 * @Copyright boulderaitech.com
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class MqMetabase2Neo4jProcessPipelineTest  extends SpringBaseTest {

    @Test
    public void testHandleDataList() {
        MqMetabase2Neo4jProcessPipeline processPipeline = new MqMetabase2Neo4jProcessPipeline(0);
        processPipeline.setNeo4jConfig(neo4jConfig);
        processPipeline.addHandlers();
        processPipeline.start();
         Gson gson = new Gson();
        String metaData="  {\"changeId\":\"da_logic_entity_54\",\"kind\":\"update\",\"schema\":\"public\",\"table\":\"da_logic_entity\",\"columnnames\":[\"id\",\"name\",\"code\",\"alias\",\"description\",\"level\",\"parent_id\",\"owners\",\"tags\",\"namespace_id\",\"status\",\"created_at\",\"updated_at\",\"created_by\",\"updated_by\",\"english_name\",\"is_main_entity\",\"table_name\",\"data_type\",\"namespace_code\",\"table_type\"],\"columntypes\":[\"bigint\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"character varying(4000)\",\"smallint\",\"bigint\",\"text[]\",\"text[]\",\"bigint\",\"integer\",\"timestamp without time zone\",\"timestamp without time zone\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"boolean\",\"character varying(500)\",\"character varying(100)\",\"character varying(100)\",\"smallint\"],\"columnvalues\":[\"1037317358193737728\",\"test3tmy\",\"test3tmy\",null,null,\"6\",\"1036837223457468416\",null,null,\"19\",\"3\",\"2022-11-02 18:48:50.616\",\"2022-11-09 09:58:36\",\"田梦垚\",\"田梦垚\",null,\"false\",\"test3tmy\",null,\"demo\",\"1\"],\"oldkeys\":{\"keynames\":[\"id\"],\"keytypes\":[\"bigint\"],\"keyvalues\":[\"1037317358193737728\"]}}  ";
        PgWalChange record = gson.fromJson(metaData, PgWalChange.class);
        PgWalChangeAck changeAck=new PgWalChangeAck(record);

        processPipeline.addChange(changeAck);
        processPipeline.handleDataList();
    }
}
