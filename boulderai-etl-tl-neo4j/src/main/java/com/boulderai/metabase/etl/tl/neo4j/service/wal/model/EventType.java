package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

/**
 * @ClassName: EventType
 * @Description: 定义数据库操作变化的类型
 * @author  df.l
 * @date 2022年10月16日
 * @Copyright boulderaitech.com
 */
public enum EventType {
    INSERT("insert"),
    UPDATE("update"),
    DELETE("delete");

    private final String text;

    private EventType(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return this.text;
    }
}
