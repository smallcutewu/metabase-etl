package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import com.boulderai.metabase.etl.tl.neo4j.BaseTest;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class InsertRowEventTest  extends BaseTest {

    @Test
    public  void testClear()
    {
        List<Column>  columns= makeColumnList();
        List<Column> primaryKeyColumns= makeColumnList();
        InsertRowEvent  event =new  InsertRowEvent();
        event.setColumns(columns);
        event.setPrimaryKeyColumns(primaryKeyColumns);
        event.clear();

        assertNull(event.getPrimaryKeyColumns());
        assertNull(event.getColumns());
    }

    @Test
    public  void testtEventType()
    {
        InsertRowEvent  event =new  InsertRowEvent();
        assertEquals(event.getEventType().toString(),"insert");
    }
}
