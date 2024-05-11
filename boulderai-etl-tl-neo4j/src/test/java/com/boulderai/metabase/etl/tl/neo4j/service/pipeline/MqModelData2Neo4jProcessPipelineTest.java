package com.boulderai.metabase.etl.tl.neo4j.service.pipeline;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWal2JsonRecord;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;
import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @ClassName: MqModelData2Neo4jProcessPipelineTest
 * @Description: 数据模型pipeline类
 * @author  df.l
 * @date 2022年09月07日
 * @Copyright boulderaitech.com
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class MqModelData2Neo4jProcessPipelineTest extends SpringBaseTest {

    @Test
    public void testHandleDataList() {
        MqModelData2Neo4jProcessPipeline processPipeline = new MqModelData2Neo4jProcessPipeline(0);
        processPipeline.setNeo4jConfig(neo4jConfig);
        processPipeline.addHandlers();
        processPipeline.start();
        PgWal2JsonRecord changeRec=this.readUpdateBadScenceJson();
        PgWalChangeAck changeAck=new PgWalChangeAck(changeRec.getChange().get(0));
        processPipeline.addChange(changeAck);
        processPipeline.handleDataList();
        Assert.assertNotNull(changeRec.getChange());
    }
}
