package com.boulderai.metabase.etl.tl.neo4j.service.handler;

import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWal2JsonRecord;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.BaseTest;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepository;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepositoryContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultDataHandlerTest  extends BaseTest {

    @Test
    public  void  testProcess()
    {
        DefaultDataHandler  handler=new DefaultDataHandler();
        PgWal2JsonRecord record = readUpdateScenceJson();
        List<PgWalChange> change =record.getChange();
//        handler.process(change);
//        Assert.assertEquals(change.size(),1);
    }

    @Test
    public  void  testIsDeleteRecordFalse()
    {
        DefaultDataHandler  handler=new DefaultDataHandler();
        PgWal2JsonRecord record = readUpdateScenceJson();

        Boolean resBool=handler.isDeleteRecord( record.getChange().get(0));
        Assert.assertFalse(resBool);
    }

    @Test
    public  void  testDeleteObject() {
        int pipelineId = 1;
        DefaultDataHandler handler = new DefaultDataHandler();
        handler.setPipelineId(pipelineId);
        Neo4jConfig neo4jConfig= createNeo4jConfig();
        Neo4jDataRepository res=new Neo4jDataRepository();
//        res.setNeo4jConfig(neo4jConfig);
        res.init();

        Neo4jDataRepositoryContext.set(res,pipelineId);
        handler.deleteObject(10L);
        Neo4jDataRepositoryContext.remove(pipelineId);
    }

    @Test
    public  void testNameEqual()
    {
        DefaultDataHandler handler=new  DefaultDataHandler  ();
        Assert.assertEquals(handler.name(),IDataHandler.DEFAULT_DATA_HANDLER_NAME);

    }

}
