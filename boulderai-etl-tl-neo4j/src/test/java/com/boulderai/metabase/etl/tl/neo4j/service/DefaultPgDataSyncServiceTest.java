package com.boulderai.metabase.etl.tl.neo4j.service;

import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultPgDataSyncServiceTest  extends SpringBaseTest {

    @Test
    public  void   testInit()
    {
       Boolean res= defaultPgDataSyncService.init();
        for(int i=0;i<10;i++)
        {
            System.out.println("wait for stream coming "+i);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Assert.assertTrue(res);

    }

}
