package com.boulderai.metabase.etl.tl.neo4j.util.pool.neo4j;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * @ClassName: Neo4jPool
 * @Description: neo4j连接池
 * @author  df.l
 * @date 2023年02月11日
 * @Copyright boulderaitech.com
 */
public class Neo4jPool extends BasePool<Neo4jSession> {

    public Neo4jPool(){

    }

    public Neo4jPool(final GenericObjectPoolConfig poolConfig, final String uri,
                     final String user, final String password, final String clientName) {
        super(poolConfig, new Neo4jSessionFactory(uri,  user, password, clientName));
    }

    @Override
    public Neo4jSession getResource() {
        Neo4jSession session = super.getResource();
        session.setDataSource(this);
        return session;
    }

    @Override
    protected void returnBrokenResource(Neo4jSession resource) {
        super.returnResource(resource);
    }

    @Override
    protected void returnResource(Neo4jSession resource) {
        super.returnResource(resource);
    }
}
