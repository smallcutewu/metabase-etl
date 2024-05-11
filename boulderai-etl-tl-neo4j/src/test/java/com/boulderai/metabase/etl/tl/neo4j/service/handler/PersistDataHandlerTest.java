package com.boulderai.metabase.etl.tl.neo4j.service.handler;

import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
//import com.boulderai.metabase.sync.core.service.handler.neo4j.PersistDataHandler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class PersistDataHandlerTest extends SpringBaseTest {


    @Test
    public  void testUpdateEventProcess()
    {
//        PersistDataHandler handler=new  PersistDataHandler  ();
//        PgWal2JsonRecord record = readUpdateWalJson();
//        List<PgWalChange> change =record.getChange();
//        TransformDataHandler trHandler=new TransformDataHandler();
//        trHandler.process(change);
//
//        handler.process(change);
//
//        Assert.assertEquals( handler.getCqlValueListMap().size(),1);
//        Assert.assertEquals( handler.getNeo4jOperCqlMap().size(),1);


    }

    @Test
    public  void testInsertEventProcess()
    {
//        PersistDataHandler  handler=new  PersistDataHandler  ();
//        PgWal2JsonRecord record = readInsertWalJson();
//
//        List<PgWalChange> change =record.getChange();
//        TransformDataHandler  trHandler=new TransformDataHandler();
//        trHandler.process(change);
//
//        handler.process(change) ;
//
//        Assert.assertEquals( handler.getCqlValueListMap().size(),1);
//        Assert.assertEquals( handler.getNeo4jOperCqlMap().size(),1);

    }


    @Test
    public  void testDeleteEventProcess()
    {
//        PersistDataHandler  handler=new  PersistDataHandler  ();
//        PgWal2JsonRecord record = readDeleteWalJson();
//
//        List<PgWalChange> change =record.getChange();
//        TransformDataHandler  trHandler=new TransformDataHandler();
//        trHandler.process(change);
//
//        handler.process(change) ;
//
//        Assert.assertEquals( handler.getCqlValueListMap().size(),1);
//        Assert.assertEquals( handler.getNeo4jOperCqlMap().size(),1);

    }
}
