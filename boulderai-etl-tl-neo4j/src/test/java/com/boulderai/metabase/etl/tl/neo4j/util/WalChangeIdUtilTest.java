package com.boulderai.metabase.etl.tl.neo4j.util;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.atomic.AtomicLong;
@RunWith(MockitoJUnitRunner.Silent.class)
public class WalChangeIdUtilTest {

    @Test
    public void testMixIdGeneralChangeId()
    {
        Long  changeId=2L;
        AtomicLong changeIdFlag=new AtomicLong(changeId);
        WalChangeIdUtil.setChangeIdFlag(changeIdFlag);
        long cid=WalChangeIdUtil.generalChangeId();

        Assert.assertEquals(cid,3L);
        changeId=9999999L+100;
        changeIdFlag=new AtomicLong(changeId);
        WalChangeIdUtil.setChangeIdFlag(changeIdFlag);

        cid=WalChangeIdUtil.generalChangeId();
    }


    @Test
    public void testMaxIdGeneralChangeId()
    {
        Long  changeId=9999999L+100;
        AtomicLong changeIdFlag=new AtomicLong(changeId);
        WalChangeIdUtil.setChangeIdFlag(changeIdFlag);
        long cid=WalChangeIdUtil.generalChangeId();

        Assert.assertEquals(cid,1L);

    }
}
