package com.boulderai.metabase.etl.tl.neo4j.zookeeper;

import com.boulderai.metabase.etl.tl.neo4j.config.ZookeeperConfig;
import com.boulderai.metabase.lang.util.SleepUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ApplicationStateWatcherTest {
   private  MetaLeaderSelector  watcher ;

    @Before
    public void before()
    {
        ZookeeperConfig pgWalConfig=new ZookeeperConfig();
        pgWalConfig.setZookeeperUrl("172.16.5.48:2181");
        pgWalConfig.setMasterNodeName("/test/mastertest");
        pgWalConfig.setConnectionTimeout(40000);
        watcher=new MetaLeaderSelector(pgWalConfig);
    }

    @After
    public void after()
    {
       if(watcher.getConnectedZk()&&watcher!=null)
       {
           watcher.stop();
       }
    }

    @Test
    public void testStart()
    {
        watcher.start();
        Boolean bl= watcher.getConnectedZk();
        Assert.assertTrue(bl);
    }

    @Test
    public void testStop()
    {
        if(watcher!=null)
        {
            watcher.start();
            SleepUtil.sleepSecond(3);
            watcher.stop();
            SleepUtil.sleepSecond(3);
            Boolean bl= watcher.getConnectedZk();
            Assert.assertFalse(bl);
        }

    }

//    @Test
//    public void testProcessEventNode()
//    {
//        Watcher.Event.EventType eventType= Watcher.Event.EventType.None;
//        Watcher.Event.KeeperState keeperState= Watcher.Event.KeeperState.SyncConnected;
//        String path="/test/test" ;
//        WatchedEvent event=new WatchedEvent( eventType, keeperState,  path);
//        watcher.process(event);
//        Boolean bl=  watcher.isInMaster();
//        Assert.assertFalse(bl);
//    }
//
//    @Test
//    public void testProcessEventNodeConnectedReadOnly()
//    {
//        Watcher.Event.EventType eventType= Watcher.Event.EventType.None;
//        Watcher.Event.KeeperState keeperState= Watcher.Event.KeeperState.ConnectedReadOnly;
//        String path="/test/test" ;
//        WatchedEvent event=new WatchedEvent( eventType, keeperState,  path);
//        watcher.process(event);
//        Boolean bl=  watcher.isInMaster();
//        Assert.assertFalse(bl);
//    }


//    @Test
//    public void testProcessEventNodeDeletedNode()
//    {
//        Watcher.Event.EventType eventType= Watcher.Event.EventType.NodeDeleted;
//        Watcher.Event.KeeperState keeperState= Watcher.Event.KeeperState.ConnectedReadOnly;
//        String path="/test/test" ;
//        WatchedEvent event=new WatchedEvent( eventType, keeperState,  path);
//        watcher.process(event);
//        Boolean bl=  watcher.isInMaster();
//        Assert.assertFalse(bl);
//    }
}
