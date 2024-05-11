package com.boulderai.metabase.etl.tl.neo4j.service.pipeline;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWal2JsonRecord;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.DefaultDataHandler;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultProcessPipelineTest extends SpringBaseTest {

    @Test
    public void testStart()
    {
        DefaultMqProcessPipeline  pipeline=new  DefaultMqProcessPipeline(10);
        pipeline.addHandlers();
        pipeline.start();
        PgWal2JsonRecord record=this.readDeleteWalJson();
        Assert.assertNotNull(record.getChange());

        for(PgWalChange walCh : record.getChange() )
        {
            PgWalChangeAck changeAck=new PgWalChangeAck(walCh);
            pipeline.addWalRecord(changeAck);
        }

        for(int i=0;i<5;i++)
        {
            System.out.println("wait for stream hanlde "+i);
            SleepUtil.sleepSecond(1);
        }

    }


    @Test
    public void testAddFirstLast()
    {
        PgWal2JsonRecord  changeRec=this.readUpdateBadScenceJson();
        DefaultDataHandler  defaultDataHandler= new DefaultDataHandler();
        DefaultMqProcessPipeline  pipeline=new  DefaultMqProcessPipeline(10);
        pipeline.addFirst(defaultDataHandler.name(),defaultDataHandler);
        pipeline.addLast(defaultDataHandler.name(),defaultDataHandler);

        PgWalChangeAck changeAck=new PgWalChangeAck(changeRec.getChange().get(0));
        List<PgWalChangeAck> dataList=new ArrayList<PgWalChangeAck>();
        dataList.add(changeAck);
        pipeline.addChange(changeAck);
        pipeline.handleWalData(dataList);
        pipeline.clearChange();
        Assert.assertNotNull(changeRec.getChange());
    }
}
