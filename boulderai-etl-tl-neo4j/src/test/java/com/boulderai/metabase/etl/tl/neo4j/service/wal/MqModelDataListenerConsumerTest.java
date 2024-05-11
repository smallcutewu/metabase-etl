package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.etl.tl.neo4j.util.StartFlagger;
import com.boulderai.metabase.etl.tl.neo4j.util.Stopper;
import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class MqModelDataListenerConsumerTest extends SpringBaseTest {
    @Test
    public void testStart()
    {
        MqModelDataListenerConsumer  consumer=new MqModelDataListenerConsumer();
        consumer.setNeo4jConfig(neo4jConfig);
        consumer.setPgWalConfig(pgWalConfig);
        consumer.start();
        int size=consumer.getPipelineQueue().size();
        Assert.assertNotEquals(0,size);
    }

    @Test
    public void testConsumerPgWalChange()
    {
        Neo4jSyncContext.getInstance().initData();
        MqModelDataListenerConsumer  consumer=new MqModelDataListenerConsumer();
        consumer.setNeo4jConfig(neo4jConfig);
        consumer.setPgWalConfig(pgWalConfig);
        consumer.start();
        int size=consumer.getPipelineQueue().size();
        Assert.assertNotEquals(0,size);
        Stopper.restart();
        StartFlagger.restart();

        String cql=" MERGE ( t:da_service_flow { id:324 })         on CREATE  set t=  {code:\"freeze_serviceFlow\",start_at:\"update\",type:\"Api\",updated_at:\"2022-11-09 14:35:41\",id:324,state:true,steps:\"[{\\\"name\\\": \\\"update\\\", \\\"next\\\": \\\"output\\\", \\\"type\\\": \\\"CallService\\\", \\\"input\\\": [{\\\"name\\\": \\\"appId\\\", \\\"javaType\\\": \\\"java.lang.String\\\", \\\"defaultValue\\\": \\\"tech\\\"}, {\\\"name\\\": \\\"modelId\\\", \\\"javaType\\\": \\\"java.lang.String\\\", \\\"defaultValue\\\": \\\"da_service_flow\\\"}, {\\\"name\\\": \\\"parameters\\\", \\\"children\\\": [{\\\"name\\\": \\\"id\\\", \\\"value\\\": \\\"$.input.id\\\", \\\"javaType\\\": \\\"java.lang.String\\\"}, {\\\"name\\\": \\\"state\\\", \\\"value\\\": \\\"$.input.state\\\", \\\"javaType\\\": \\\"java.lang.Boolean\\\"}], \\\"javaType\\\": \\\"java.lang.Object\\\"}], \\\"beanName\\\": \\\"modelDataServiceImpl\\\", \\\"methodName\\\": \\\"update\\\"}, {\\\"name\\\": \\\"output\\\", \\\"type\\\": \\\"Output\\\", \\\"result\\\": [{\\\"name\\\": \\\"result\\\", \\\"value\\\": \\\"$.flowVariables.update\\\", \\\"javaType\\\": \\\"java.lang.Object\\\"}]}]\",input:\"[{\\\"name\\\": \\\"id\\\", \\\"javaType\\\": \\\"java.lang.String\\\", \\\"jsonType\\\": \\\"STRING\\\"}, {\\\"name\\\": \\\"state\\\", \\\"javaType\\\": \\\"java.lang.Boolean\\\", \\\"jsonType\\\": \\\"STRING\\\"}]\",name:\"冻结服务编排\"}  \n" +
                "\t on MATCH  set  t= {code:\"freeze_serviceFlow\",start_at:\"update\",type:\"Api\",updated_at:\"2022-11-09 14:35:41\",id:324,state:true,steps:\"[{\\\"name\\\": \\\"update\\\", \\\"next\\\": \\\"output\\\", \\\"type\\\": \\\"CallService\\\", \\\"input\\\": [{\\\"name\\\": \\\"appId\\\", \\\"javaType\\\": \\\"java.lang.String\\\", \\\"defaultValue\\\": \\\"tech\\\"}, {\\\"name\\\": \\\"modelId\\\", \\\"javaType\\\": \\\"java.lang.String\\\", \\\"defaultValue\\\": \\\"da_service_flow\\\"}, {\\\"name\\\": \\\"parameters\\\", \\\"children\\\": [{\\\"name\\\": \\\"id\\\", \\\"value\\\": \\\"$.input.id\\\", \\\"javaType\\\": \\\"java.lang.String\\\"}, {\\\"name\\\": \\\"state\\\", \\\"value\\\": \\\"$.input.state\\\", \\\"javaType\\\": \\\"java.lang.Boolean\\\"}], \\\"javaType\\\": \\\"java.lang.Object\\\"}], \\\"beanName\\\": \\\"modelDataServiceImpl\\\", \\\"methodName\\\": \\\"update\\\"}, {\\\"name\\\": \\\"output\\\", \\\"type\\\": \\\"Output\\\", \\\"result\\\": [{\\\"name\\\": \\\"result\\\", \\\"value\\\": \\\"$.flowVariables.update\\\", \\\"javaType\\\": \\\"java.lang.Object\\\"}]}]\",input:\"[{\\\"name\\\": \\\"id\\\", \\\"javaType\\\": \\\"java.lang.String\\\", \\\"jsonType\\\": \\\"STRING\\\"}, {\\\"name\\\": \\\"state\\\", \\\"javaType\\\": \\\"java.lang.Boolean\\\", \\\"jsonType\\\": \\\"STRING\\\"}]\",name:\"冻结服务编排\"}";

        when(neo4jDataRepository.operNeo4jDataByCqlSingle(cql)).thenReturn(true);

        Boolean  success=false;
        String modelData=" {\"changeId\":\"da_service_flow_3\",\"kind\":\"update\",\"schema\":\"public\",\"table\":\"da_service_flow\",\"columnnames\":[\"id\",\"name\",\"code\",\"description\",\"start_at\",\"input\",\"output\",\"context_values\",\"steps\",\"___owned_org_id__1\",\"___owned_by__1\",\"___created_by__1\",\"___created_at__1\",\"___updated_by__1\",\"___updated_at__1\",\"___deleted_by__1\",\"___deleted_at__1\",\"___type__1\",\"___event_code__1\",\"___service_flow_type__1\",\"event\",\"type\",\"state\",\"deleted_by\",\"created_at\",\"updated_at\",\"owner_org_code\",\"deleted_at\",\"created_by\",\"updated_by\",\"corp_id\"],\"columntypes\":[\"bigint\",\"character varying(100)\",\"character varying(100)\",\"text\",\"character varying(100)\",\"jsonb\",\"jsonb\",\"jsonb\",\"jsonb\",\"character varying(50)\",\"character varying(50)\",\"character varying(50)\",\"timestamp without time zone\",\"character varying(50)\",\"timestamp without time zone\",\"character varying(50)\",\"timestamp without time zone\",\"character varying(50)\",\"character varying(50)\",\"character varying(50)\",\"character varying(50)\",\"character varying(50)\",\"boolean\",\"character varying(50)\",\"timestamp without time zone\",\"timestamp without time zone\",\"character varying(50)\",\"timestamp without time zone\",\"character varying(50)\",\"character varying(50)\",\"character varying(50)\"],\"columnvalues\":[\"324\",\"冻结服务编排\",\"freeze_serviceFlow\",null,\"update\",\"[{\\\"name\\\": \\\"id\\\", \\\"javaType\\\": \\\"java.lang.String\\\", \\\"jsonType\\\": \\\"STRING\\\"}, {\\\"name\\\": \\\"state\\\", \\\"javaType\\\": \\\"java.lang.Boolean\\\", \\\"jsonType\\\": \\\"STRING\\\"}]\",null,null,\"[{\\\"name\\\": \\\"update\\\", \\\"next\\\": \\\"output\\\", \\\"type\\\": \\\"CallService\\\", \\\"input\\\": [{\\\"name\\\": \\\"appId\\\", \\\"javaType\\\": \\\"java.lang.String\\\", \\\"defaultValue\\\": \\\"tech\\\"}, {\\\"name\\\": \\\"modelId\\\", \\\"javaType\\\": \\\"java.lang.String\\\", \\\"defaultValue\\\": \\\"da_service_flow\\\"}, {\\\"name\\\": \\\"parameters\\\", \\\"children\\\": [{\\\"name\\\": \\\"id\\\", \\\"value\\\": \\\"$.input.id\\\", \\\"javaType\\\": \\\"java.lang.String\\\"}, {\\\"name\\\": \\\"state\\\", \\\"value\\\": \\\"$.input.state\\\", \\\"javaType\\\": \\\"java.lang.Boolean\\\"}], \\\"javaType\\\": \\\"java.lang.Object\\\"}], \\\"beanName\\\": \\\"modelDataServiceImpl\\\", \\\"methodName\\\": \\\"update\\\"}, {\\\"name\\\": \\\"output\\\", \\\"type\\\": \\\"Output\\\", \\\"result\\\": [{\\\"name\\\": \\\"result\\\", \\\"value\\\": \\\"$.flowVariables.update\\\", \\\"javaType\\\": \\\"java.lang.Object\\\"}]}]\",null,null,null,null,null,null,null,null,null,null,null,null,\"Api\",\"true\",null,null,\"2022-11-09 14:35:41\",null,null,null,null,null],\"oldkeys\":{\"keynames\":[\"id\"],\"keytypes\":[\"bigint\"],\"keyvalues\":[\"324\"]}} ";
        try {
            consumer.consumerPgWalChange( modelData, 100L, null,1);
            success=true;
        }
        catch (Exception ex)
        {

        }
        Assert.assertTrue(success);
    }
}
