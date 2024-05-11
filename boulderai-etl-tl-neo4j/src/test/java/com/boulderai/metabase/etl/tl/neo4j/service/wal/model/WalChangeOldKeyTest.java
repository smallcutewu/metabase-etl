package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import com.boulderai.metabase.etl.tl.neo4j.BaseTest;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class WalChangeOldKeyTest  extends BaseTest {

    @Test
    public void testMakePrimaryKeyColumn()
    {
        PgWal2JsonRecord record = readUpdateWalJson();
        assertNotNull(record.getChange());

        List<PgWalChange> change= record.getChange();
        assertEquals(change.size(),1);
        assertEquals(change.get(0).getColumnnames().size(),6);

        WalChangeOldKey  walChangeOldKey= change.get(0).getOldkeys();
        assertNotNull(walChangeOldKey);

        List<Column> columns  = walChangeOldKey.makePrimaryKeyColumn();
        assertEquals(walChangeOldKey.getKeynames().size(),columns.size());
        assertEquals(walChangeOldKey.getKeynames().get(0),columns.get(0).getName());
    }

}
