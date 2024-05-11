package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CqlValueContainerTest {

    @Test
    public void testClear()
    {
        String cql="match n Person return n";
        EventType  eventType=EventType.INSERT;
        String  tableName="person";
        CqlValueContainer  container=new  CqlValueContainer( cql,  eventType,  tableName);
        container.clear();

        assertNull(container.getValueList());

    }
}
