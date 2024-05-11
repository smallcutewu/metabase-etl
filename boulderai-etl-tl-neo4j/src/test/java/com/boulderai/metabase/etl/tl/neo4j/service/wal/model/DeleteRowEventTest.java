package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DeleteRowEventTest {

    @Test
    public  void testtEventType()
    {
        InsertRowEvent  event =new  InsertRowEvent();
        assertEquals(event.getEventType().toString(),"delete");
    }
}
