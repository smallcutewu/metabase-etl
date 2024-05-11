package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EventTypeTest {

    @Test
    public void testToString() {
        assertEquals(EventType.DELETE.toString(), "delete");
        assertEquals(EventType.UPDATE.toString(), "update");
        assertEquals(EventType.INSERT.toString(), "insert");

    }
}
