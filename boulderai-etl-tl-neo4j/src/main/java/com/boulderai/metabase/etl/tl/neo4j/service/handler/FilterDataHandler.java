package com.boulderai.metabase.etl.tl.neo4j.service.handler;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;

/**
 * @ClassName: FilterDataHandler
 * @Description: 数据过滤处理器
 * @author  df.l
 * @date 2022年10月112日
 * @Copyright boulderaitech.com
 */
public class FilterDataHandler   extends BaseDataHandler
{

    @Override
    protected void processDetail(List<PgWalChangeAck> dataList,List<String>  cqlList) {
        //清除无效的改变
//        for (PgWalChangeAck changeAck : dataList) {
//            PgWalChange change=changeAck.getChange();
        Iterator<PgWalChangeAck> iterator = dataList.iterator();
        while (iterator.hasNext()) {
            PgWalChangeAck  changeAck=iterator.next();
            PgWalChange  change=changeAck.getChange();
            String kind = change.getKind();
            if (StringUtils.isEmpty(kind)||!change.checkChangeIsOK() ) {
                iterator.remove();
                continue;
            }
        }

    }



    @Override
    public String name() {
        return FILTER_DATA_HANDLER_NAME;
    }
}
