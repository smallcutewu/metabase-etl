package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;


/**
 * @ClassName: InsertRowEvent
 * @Description: pg wal 插入事件
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public class InsertRowEvent extends AbstractRowEvent {
    @Override
    public EventType getEventType() {
        return EventType.INSERT;
    }
}