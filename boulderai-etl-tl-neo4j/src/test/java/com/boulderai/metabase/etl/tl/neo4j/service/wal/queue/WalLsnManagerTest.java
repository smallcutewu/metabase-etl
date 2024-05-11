package com.boulderai.metabase.etl.tl.neo4j.service.wal.queue;

import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class WalLsnManagerTest extends SpringBaseTest {

    @Test
    public void  testSaveLastLsn()
    {
        WalLsnManager  walLsnManager=  WalLsnManager.getInstance().setSlotName("fff_test");
        walLsnManager.init();
        walLsnManager.setLastLsn("29/E83C0585");
       Boolean res= walLsnManager.saveLastLsn();
        Assert.assertTrue(res);
    }
}
