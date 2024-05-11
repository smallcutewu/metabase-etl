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
public class LastDataHandlerTest  extends BaseTest {

    @Test
    public  void  testProcess()
    {
        LastDataHandler handler=new  LastDataHandler  ();
        PgWal2JsonRecord record = readUpdateScenceJson();
        List<PgWalChange> change =record.getChange();
//        handler.process(change);
//        Assert.assertEquals(change.size(),0);
    }

    @Test
    public  void testNameEqual()
    {
        LastDataHandler handler=new  LastDataHandler  ();
        Assert.assertEquals(handler.name(),IDataHandler.LAST_DATA_HANDLER_NAME);

    }
}
