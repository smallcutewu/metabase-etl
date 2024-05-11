package com.boulderai.metabase.etl.tl.neo4j.zookeeper;

import com.boulderai.metabase.etl.tl.neo4j.util.AppCurrentState;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(MockitoJUnitRunner.Silent.class)
public class MetaLeaderSelectorTest   extends SpringBaseTest {



    @Test
    public void testStart()
    {
        final AtomicBoolean  isLeader=new AtomicBoolean(false);
        MetaLeaderSelector  selector=new MetaLeaderSelector(this.zookeeperConfig);
        AppStateListener listener = new AppStateListener() {
            @Override
            public void onChange(AppCurrentState state) {
                switch (state)
                {
                    case MASTER:
                        isLeader.set(true);
                        break;
                    case SLAVE:
                        isLeader.set(false);
                        break;
                }
            }
        };

        selector.setListener(listener);
        selector.start();

        SleepUtil.sleepSecond(3);
        Assert.assertTrue(isLeader.get());
        selector.stop();
    }


    @Test
    public void testStop()
    {
        final AtomicBoolean  isLeader=new AtomicBoolean(false);
        MetaLeaderSelector  selector=new MetaLeaderSelector(this.zookeeperConfig);
        AppStateListener listener = new AppStateListener() {
            @Override
            public void onChange(AppCurrentState state) {
                switch (state)
                {
                    case MASTER:
                        isLeader.set(true);
                        break;
                    case SLAVE:
                        isLeader.set(false);
                        break;
                }
            }
        };

        selector.setListener(listener);
        selector.start();

        SleepUtil.sleepSecond(3);
        selector.stop();
        SleepUtil.sleepSecond(2);
        Assert.assertFalse(selector.getConnectedZk());
    }
}
