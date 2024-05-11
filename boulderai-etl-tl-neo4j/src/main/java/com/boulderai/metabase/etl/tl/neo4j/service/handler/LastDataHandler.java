package com.boulderai.metabase.etl.tl.neo4j.service.handler;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * @ClassName: LastDataHandler
 * @Description: 末尾消息处理器
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public class LastDataHandler  extends BaseDataHandler {
    @Override
    protected void processDetail(List<PgWalChangeAck> dataList,List<String>  cqlList) {
        if(!CollectionUtils.isEmpty(dataList))
        {
            for (PgWalChangeAck changeAck : dataList) {
                PgWalChange change=changeAck.getChange();
                change.clear();
            }
            dataList.clear();
        }
    }

    @Override
    protected void exceptionCaught(Exception ex) {
        super.exceptionCaught(ex);
    }

    @Override
    public String name() {
        return LAST_DATA_HANDLER_NAME;
    }
}
