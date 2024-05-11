package com.boulderai.metabase.etl.tl.neo4j.util;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class StopperTest {
    @Test
    public void testStop()
    {
        Stopper  stopper=new Stopper();
        Assert.assertFalse(Stopper.isStopped());
        Assert.assertTrue(Stopper.isRunning());

//        Stopper.stop();

        Assert.assertTrue(Stopper.isStopped());
        Assert.assertFalse(Stopper.isRunning());
    }
}
