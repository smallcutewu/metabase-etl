package com.boulderai.metabase.etl.tl.neo4j.service.handler;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWal2JsonRecord;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.BaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

@RunWith(MockitoJUnitRunner.Silent.class)
public class FilterDataHandlerTest  extends BaseTest {

    @Test
    public  void  testProcess()
    {
        FilterDataHandler handler=new  FilterDataHandler  ();
        PgWal2JsonRecord record = readUpdateScenceJson();
        List<PgWalChange> change =record.getChange();
        PgWalChange  change2=new PgWalChange();
        change.add(change2);
//        handler.process(change);
//        Assert.assertEquals(change.size(),1);
    }


    @Test
    public  void  testProcessBadRecord()
    {
        FilterDataHandler handler=new  FilterDataHandler  ();
        PgWal2JsonRecord record = readUpdateBadScenceJson();
        List<PgWalChange> change =record.getChange();
        PgWalChange  change2=new PgWalChange();
        change.add(change2);
//        handler.process(change);
//        Assert.assertEquals(change.size(),0);
    }

    @Test
    public  void testNameEqual()
    {
        FilterDataHandler handler=new  FilterDataHandler  ();
        Assert.assertEquals(handler.name(),IDataHandler.FILTER_DATA_HANDLER_NAME);

    }

}
