package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.etl.tl.neo4j.util.StartFlagger;
import com.boulderai.metabase.etl.tl.neo4j.util.Stopper;
import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class MqMetabaseListenerConsumerTest extends SpringBaseTest {

    @Test
    public void testStart()
    {
        MqMetabaseListenerConsumer  mqMetabaseListenerConsumer=new MqMetabaseListenerConsumer();
        mqMetabaseListenerConsumer.setNeo4jConfig(neo4jConfig);
        mqMetabaseListenerConsumer.setPgWalConfig(pgWalConfig);
        Mockito.when(applicationContext.getBean(MqMetabaseListenerConsumer.class)).thenReturn(mqMetabaseListenerConsumer);

        mqMetabaseListenerConsumer.start();
        int size=mqMetabaseListenerConsumer.getPipelineQueue().size();
        Assert.assertNotEquals(0,size);
    }


    @Test
    public void testConsumerPgWalChange()
    {
        Neo4jSyncContext.getInstance().initData();
        MqMetabaseListenerConsumer  mqMetabaseListenerConsumer=new MqMetabaseListenerConsumer();
        mqMetabaseListenerConsumer.setNeo4jConfig(neo4jConfig);
        mqMetabaseListenerConsumer.setPgWalConfig(pgWalConfig);
        Mockito.when(applicationContext.getBean(MqMetabaseListenerConsumer.class)).thenReturn(mqMetabaseListenerConsumer);

        mqMetabaseListenerConsumer.start();
        int size=mqMetabaseListenerConsumer.getPipelineQueue().size();
        Assert.assertNotEquals(0,size);
        Stopper.restart();
        StartFlagger.restart();
        Boolean  success=false;
        String metaData="  {\"changeId\":\"da_logic_entity_54\",\"kind\":\"update\",\"schema\":\"public\",\"table\":\"da_logic_entity\",\"columnnames\":[\"id\",\"name\",\"code\",\"alias\",\"description\",\"level\",\"parent_id\",\"owners\",\"tags\",\"namespace_id\",\"status\",\"created_at\",\"updated_at\",\"created_by\",\"updated_by\",\"english_name\",\"is_main_entity\",\"table_name\",\"data_type\",\"namespace_code\",\"table_type\"],\"columntypes\":[\"bigint\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"character varying(4000)\",\"smallint\",\"bigint\",\"text[]\",\"text[]\",\"bigint\",\"integer\",\"timestamp without time zone\",\"timestamp without time zone\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"boolean\",\"character varying(500)\",\"character varying(100)\",\"character varying(100)\",\"smallint\"],\"columnvalues\":[\"1037317358193737728\",\"test3tmy\",\"test3tmy\",null,null,\"6\",\"1036837223457468416\",null,null,\"19\",\"3\",\"2022-11-02 18:48:50.616\",\"2022-11-09 09:58:36\",\"田梦垚\",\"田梦垚\",null,\"false\",\"test3tmy\",null,\"demo\",\"1\"],\"oldkeys\":{\"keynames\":[\"id\"],\"keytypes\":[\"bigint\"],\"keyvalues\":[\"1037317358193737728\"]}}  ";
        try {
            mqMetabaseListenerConsumer.consumerPgWalChange( metaData, 100L, null);
            success=true;
        }
        catch (Exception ex)
        {

        }
        Assert.assertTrue(success);
    }
}
