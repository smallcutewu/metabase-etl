package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class Neo4jSyncContextTest extends SpringBaseTest {

    @Test
    public void testInitData()
    {
        Boolean  success=false;
        try {
            Neo4jSyncContext.getInstance().initData();
            success=true;
        }
        catch (Exception ex)
        {
        }
        Neo4jSyncContext.getInstance().getModelDataConfigRelationByTable("md_spart_product_base_info",2);
        Assert.assertTrue(success);
    }

//    @Test
//    public void testGetModelDataRelationByTable()
//    {
//        Boolean  success=false;
//        try {
//            Neo4jSyncContext.getInstance().initData();
//            Neo4jSyncContext.getInstance().getModelDataRelationByTable("ltc_lead_product_info");
//            success=true;
//        }
//        catch (Exception ex)
//        {
//        }
//        Assert.assertTrue(success);
//    }
}
