package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;


/**
 * @ClassName: UpdateRowEvent
 * @Description: pg wal 插入事件
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public class UpdateRowEvent extends AbstractRowEvent {
    @Override
    public EventType getEventType() {
        return EventType.UPDATE;
    }
}
