package com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j;

import com.boulderai.metabase.etl.tl.neo4j.service.handler.BaseDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.IDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * @ClassName: PersistDataHandler
 * @Description: 数据转换处理器
 * @author  df.l
 * @date 2022年10月12日
 * @Copyright boulderaitech.com
 */
public class TransformDataHandler   extends BaseDataHandler {

    private  final static Logger logger = LoggerFactory.getLogger(TransformDataHandler.class);

    @Override
    protected void processDetail(List<PgWalChangeAck> dataList,List<String>  cqlList) {
//        List<AbstractRowEvent> rows = new ArrayList<AbstractRowEvent>();
        //        for (PgWalChangeAck changeAck : dataList) {
//            PgWalChange change=changeAck.getChange();

        for(PgWalChangeAck  changeAck: dataList)
        {
            PgWalChange change=changeAck.getChange();
            AbstractRowEvent  row=null;
            String kind=change.getKind();
            if (kind.equals(EventType.INSERT.toString())) {
                row=doInsertEvent(  change) ;
            } else if (kind.equals(EventType.UPDATE.toString())) {
                row=doUpdateEvent(  change );
            } else if (kind.equals(EventType.DELETE.toString())) {
                row=doDeleteEvent(  change) ;
            }

            if(row!=null)
            {
                change.setChangeToRow(row);
            }

        }


    }



    public InsertRowEvent doInsertEvent(PgWalChange  change) {
        InsertRowEvent rowEvent = new InsertRowEvent();
        setMeta(rowEvent,change);
        rowEvent.setColumns(change.makeColumn());
        return rowEvent;
    }

    public UpdateRowEvent doUpdateEvent(PgWalChange  change){
        UpdateRowEvent rowEvent = new UpdateRowEvent();
        setMeta(rowEvent,change);
        rowEvent.setColumns(change.makeColumn());
        if(change.getOldkeys()!=null)
        {
            rowEvent.setPrimaryKeyColumns(change.getOldkeys().makePrimaryKeyColumn());
        }
        return rowEvent;
    }


    public DeleteRowEvent doDeleteEvent(PgWalChange  change) {
        DeleteRowEvent rowEvent = new DeleteRowEvent();
        setMeta(rowEvent,change);
        if(change.getOldkeys()!=null)
        {
            rowEvent.setPrimaryKeyColumns(change.getOldkeys().makePrimaryKeyColumn());
        }
        return rowEvent;
    }


    private void setMeta(AbstractRowEvent rowEvent,PgWalChange  change) {
        rowEvent.setSchemaName(change.getSchema());
        rowEvent.setTableName(change.getTable());
    }





    @Override
    public String name() {
        return IDataHandler.TRANSFORM_DATA_HANDLER_NAME;
    }
}
