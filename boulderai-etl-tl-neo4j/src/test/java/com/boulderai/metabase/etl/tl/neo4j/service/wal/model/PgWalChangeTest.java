package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import com.boulderai.metabase.etl.tl.neo4j.BaseTest;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class PgWalChangeTest extends BaseTest {

    @Test
    public void testMakeColumn()
    {
        PgWal2JsonRecord record = readUpdateWalJson();
        assertNotNull(record);
        assertNotNull(record.getChange());

        List<PgWalChange>  change= record.getChange();
        assertEquals(change.size(),1);
        assertEquals(change.get(0).getColumnnames().size(),6);

        List<Column> columns  = change.get(0).makeColumn();
        assertEquals(change.get(0).getColumnnames().size(),columns.size());
        assertEquals(change.get(0).getColumnnames().get(0),columns.get(0).getName());
    }

    @Test
    public void testClear() {
        PgWal2JsonRecord record = readUpdateWalJson();
        assertNotNull(record);
        record.getChange().get(0).clear();
        assertNull(record.getChange().get(0).getColumnnames());
    }

    @Test
    public void testMakeOneColumn()
    {
        PgWal2JsonRecord record = readUpdateWalJson();
        assertNotNull(record);
        assertNotNull(record.getChange());

        List<PgWalChange>  change= record.getChange();
        assertEquals(change.size(),1);
        Column column  = change.get(0).makeOneColumn("name");
        assertNotNull(column);

        column= change.get(0).getOldkeys().makeOneColumn();
        assertNotNull(column);

        column= change.get(0).getOldkeys().makeOneColumn("id");
        assertNotNull(column);
    }


}
