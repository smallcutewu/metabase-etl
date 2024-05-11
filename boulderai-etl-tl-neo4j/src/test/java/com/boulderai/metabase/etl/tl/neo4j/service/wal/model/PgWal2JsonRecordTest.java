package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import com.boulderai.metabase.etl.tl.neo4j.BaseTest;
import org.junit.Test;

import static org.junit.Assert.*;

public class PgWal2JsonRecordTest extends BaseTest {

    @Test
    public void testMakeColumn()
    {
        PgWal2JsonRecord record = readUpdateWalJson();
        assertNotNull(record);
        assertNotNull(record.getChange());
        assertNotNull(record.getNextlsn());
        assertNotNull(record.getXid());



    }

    @Test
    public void testClear() {
        PgWal2JsonRecord record = readUpdateWalJson();
        assertNotNull(record);
        record.clear();
        assertNull(record.getChange());
    }


}
