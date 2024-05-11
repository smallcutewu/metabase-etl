package com.boulderai.metabase.etl.tl.neo4j.util;

import com.boulderai.metabase.etl.tl.neo4j.BaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class AppCurrentStateTest  extends BaseTest {

    @Test
    public void getGetStatusByMaster()
    {
        AppCurrentState  state=AppCurrentState.MASTER;
        Assert.assertEquals(1,state.getStatus());
    }

    @Test
    public void getGetStatusBySlaver()
    {
        AppCurrentState  state=AppCurrentState.SLAVE;
        Assert.assertEquals(0,state.getStatus());
    }
}
