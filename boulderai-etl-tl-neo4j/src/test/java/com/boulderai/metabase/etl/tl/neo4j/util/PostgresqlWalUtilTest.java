package com.boulderai.metabase.etl.tl.neo4j.util;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class PostgresqlWalUtilTest {

    @Test
    public  void testHstoreToJson()
    {
        String hsStr="99.88m";
        String  json=  PostgresqlWalUtil.extractNumeric(hsStr);
        Assert.assertEquals("99.88",json);
    }
}
