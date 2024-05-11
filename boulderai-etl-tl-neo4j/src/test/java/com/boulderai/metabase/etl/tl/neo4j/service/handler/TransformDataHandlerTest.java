package com.boulderai.metabase.etl.tl.neo4j.service.handler;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWal2JsonRecord;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.UpdateRowEvent;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.TransformDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.BaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
@RunWith(MockitoJUnitRunner.Silent.class)
public class TransformDataHandlerTest extends BaseTest {

    @Test
    public  void testUpdateEventProcess()
    {
        TransformDataHandler handler=new  TransformDataHandler  ();
        PgWal2JsonRecord record = readUpdateScenceJson();
        List<PgWalChange> change =record.getChange();
//        handler.process(change);
//        for(PgWalChange  pgWalch:  change)
//        {
//            AbstractRowEvent changeToRow=  pgWalch.getChangeToRow();
//            Assert.assertNotNull(changeToRow);
//            Assert.assertEquals(changeToRow.getEventType().toString(),"update");
//        }

    }

    @Test
    public  void testInsertEventProcess()
    {
        TransformDataHandler  handler=new  TransformDataHandler  ();
        PgWal2JsonRecord record = readInsertWalJson();
        List<PgWalChange> change =record.getChange();
//        handler.process(change);
//        for(PgWalChange  pgWalch:  change)
//        {
//            AbstractRowEvent changeToRow=  pgWalch.getChangeToRow();
//            Assert.assertNotNull(changeToRow);
//            Assert.assertEquals(changeToRow.getEventType().toString(),"insert");
//        }

    }


    @Test
    public  void testDeleteEventProcess()
    {
        TransformDataHandler  handler=new  TransformDataHandler  ();
        PgWal2JsonRecord record = readDeleteWalJson();
        List<PgWalChange> change =record.getChange();
//        handler.process(change);
//        for(PgWalChange  pgWalch:  change)
//        {
//            AbstractRowEvent changeToRow=  pgWalch.getChangeToRow();
//            Assert.assertNotNull(changeToRow);
//            Assert.assertEquals(changeToRow.getEventType().toString(),"delete");
//        }

    }


    @Test
    public  void testDoUpdateEvent()
    {
        TransformDataHandler handler=new  TransformDataHandler  ();
        PgWal2JsonRecord record = readUpdateScenceJson();
        List<PgWalChange> change =record.getChange();
        UpdateRowEvent event=handler.doUpdateEvent(change.get(0));
        Assert.assertNotNull(event);

    }


    @Test
    public  void testNameEqual()
    {
        TransformDataHandler handler=new  TransformDataHandler  ();
        Assert.assertEquals(handler.name(),IDataHandler.TRANSFORM_DATA_HANDLER_NAME);

    }




}