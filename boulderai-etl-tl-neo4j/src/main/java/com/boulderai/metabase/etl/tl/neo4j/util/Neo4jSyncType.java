package com.boulderai.metabase.etl.tl.neo4j.util;

/**
 * @ClassName: Neo4jSyncType
 * @Description: neo4j同步类型定义
 * @author  df.l
 * @date 2022年09月05日
 * @Copyright boulderaitech.com
 */
public enum Neo4jSyncType {
    /**
     * 同步整个表node
     */
    OnjectSyn("object"),

    /**
     * 同步关系relation关系
     */
    RelationSyn("relation");

    private final String type;

    private Neo4jSyncType(String type)
    {
        this.type=type;
    }

    @Override
    public String toString() {
        return this.type;
    }
}
