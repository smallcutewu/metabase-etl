package com.boulderai.metabase.etl.tl.neo4j.util;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetAddress;
import java.net.UnknownHostException;
@RunWith(MockitoJUnitRunner.Silent.class)
public class NetUtilsTest {

    @Test
    public void testGetLocalAddress()
    {
        String ip=NetUtils.getLocalAddress();
        Assert.assertNotNull(ip);
    }

    @Test
    public void testNormalizeHostAddress()
    {
        InetAddress localHost = null;
        try {
            localHost = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        String ip=NetUtils.normalizeHostAddress(localHost);
        Assert.assertNotNull(ip);
    }


}
