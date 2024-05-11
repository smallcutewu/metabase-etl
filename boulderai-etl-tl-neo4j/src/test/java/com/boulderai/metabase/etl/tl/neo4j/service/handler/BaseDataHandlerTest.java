package com.boulderai.metabase.etl.tl.neo4j.service.handler;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWal2JsonRecord;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;
import com.boulderai.metabase.lang.exception.MetabaseException;
import com.boulderai.metabase.etl.tl.neo4j.BaseTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.Silent.class)
public class BaseDataHandlerTest  extends BaseTest {

    @Test(expected = MetabaseException.class)
    public  void  testThrowMetabaseException()
    {
        BaseDataHandler  handler= new BaseDataHandler() {
            @Override
            public String name() {
                return "test";
            }

            @Override
            protected void processDetail(List<PgWalChangeAck> dataList, List<String>  cqlList) {
                throw  new RuntimeException("单元测试故意抛出异常");
            }
        };
        PgWal2JsonRecord record = readUpdateScenceJson();
        PgWalChangeAck  pgWalChangeAck=new PgWalChangeAck(record.getChange().get(0));
        List<PgWalChangeAck> dataList=new ArrayList<PgWalChangeAck>();
        dataList.add(pgWalChangeAck);
        List<String>  cqlList=new ArrayList<String>();
        handler.process(dataList,cqlList);

    }
}
