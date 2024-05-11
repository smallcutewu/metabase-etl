package com.boulderai.metabase.etl.tl.neo4j.service.handler;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.Neo4jSyncContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWal2JsonRecord;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.meta.PersistMetabaseDataHandler;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.Silent.class)
public class PersistMetabaseDataHandlerTest extends SpringBaseTest {

    @Before
    public void before() throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        useMockNeo4j=true;
        super.before();

    }

    public void testHan()
    {
        Neo4jSyncContext neo4jSyncContext =Neo4jSyncContext.getInstance();
        List<PgWalChange> dataList=new ArrayList<PgWalChange>();
        PgWal2JsonRecord rec=this.readUpdateWalJson();
        PersistMetabaseDataHandler handler=new PersistMetabaseDataHandler();
    }
}
