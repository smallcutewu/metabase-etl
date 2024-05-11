package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;



/**
 * @ClassName: DeleteRowEvent
 * @Description: pg wal 行删除事件
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */

public class DeleteRowEvent extends AbstractRowEvent {
    @Override
    public EventType getEventType() {
        return EventType.DELETE;
    }
}
