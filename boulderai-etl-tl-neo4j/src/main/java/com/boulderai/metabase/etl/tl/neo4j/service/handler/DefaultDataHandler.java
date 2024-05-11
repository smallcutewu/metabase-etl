package com.boulderai.metabase.etl.tl.neo4j.service.handler;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;

import java.util.List;

/**
 * @ClassName: DefaultDataHandler
 * @Description: 默认数据处理器
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public class DefaultDataHandler   extends    BaseDataHandler{


    @Override
    protected  void processDetail(List<PgWalChangeAck> dataList,List<String>  cqlList)
    {

    }



    @Override
    public String name() {
        return DEFAULT_DATA_HANDLER_NAME;
    }
}
