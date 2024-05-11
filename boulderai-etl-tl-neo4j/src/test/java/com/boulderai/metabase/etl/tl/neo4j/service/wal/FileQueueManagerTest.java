package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.context.env.DefaultEnvironment;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWal2JsonRecord;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.etl.tl.neo4j.BaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class FileQueueManagerTest  extends BaseTest {

    @Test
    public void  testSendProduct()
    {
        DefaultEnvironment.registerUnitTest();
        FileQueueManager.getInstance().start();
        PgWal2JsonRecord changeRec=this.readInsertWalJson();
        String queueName= DataSynConstant.WAL_MODEL_DATA_QUEUE_NAME;
        FileQueueManager.getInstance().sendProduct(changeRec.getChange().get(0), queueName);
        Boolean startFlag=FileQueueManager.getInstance().getStartOk();

       String msg= FileQueueManager.getInstance().printTotalPerfile();
        FileQueueManager.getInstance().close();
        Assert.assertTrue(startFlag);
        Assert.assertNotNull(msg);
    }

}
