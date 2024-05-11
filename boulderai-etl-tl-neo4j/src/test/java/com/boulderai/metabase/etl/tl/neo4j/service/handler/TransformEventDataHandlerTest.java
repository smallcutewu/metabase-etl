package com.boulderai.metabase.etl.tl.neo4j.service.handler;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.DeleteRowEvent;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.InsertRowEvent;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWal2JsonRecord;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.TransformEventDataHandler;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TransformEventDataHandlerTest extends SpringBaseTest {


    @Test
    public  void testUpdateEventProcess()
    {
        TransformEventDataHandler handler=new  TransformEventDataHandler  ();
        PgWal2JsonRecord record = readUpdateScenceJson();
        List<PgWalChange> changeList =record.getChange();
//
//        handler.process(changeList);
//
//        for (PgWalChange change : changeList) {
//            Assert.assertNotNull(change.getChangeToRow());
//            AbstractRowEvent changeToRow=  change.getChangeToRow();
//            Assert.assertEquals(changeToRow.getEventType().toString(),"update");
//        }



    }


    @Test
    public  void testNameEqual()
    {
        TransformEventDataHandler handler=new  TransformEventDataHandler  ();
        Assert.assertEquals(handler.name(),IDataHandler.TRANSFORM_DATA_HANDLER_NAME);

    }


    @Test
    public  void testInsertEventProcess()
    {
        TransformEventDataHandler  handler=new  TransformEventDataHandler  ();
        PgWal2JsonRecord record = readInsertWalJson();

        List<PgWalChange> change =record.getChange();

        InsertRowEvent insertRowEvent= handler.doInsertEvent(change.get(0));
        Assert.assertNotNull(insertRowEvent);

    }


    @Test
    public  void testDeleteEventProcess()
    {
        TransformEventDataHandler  handler=new  TransformEventDataHandler  ();
        PgWal2JsonRecord record = readInsertWalJson();

        List<PgWalChange> change =record.getChange();

        DeleteRowEvent deleteRowEvent= handler.doDeleteEvent(change.get(0));
        Assert.assertNotNull(deleteRowEvent);

    }
}
