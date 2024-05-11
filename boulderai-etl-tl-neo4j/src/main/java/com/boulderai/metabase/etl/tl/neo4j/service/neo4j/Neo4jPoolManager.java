package com.boulderai.metabase.etl.tl.neo4j.service.neo4j;

import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.boulderai.metabase.etl.tl.neo4j.util.pool.neo4j.Neo4jPool;
import com.boulderai.metabase.etl.tl.neo4j.util.pool.neo4j.Neo4jSession;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class Neo4jPoolManager {
    private  Neo4jPool neo4jPool;
    private Neo4jConfig neo4jConfig;


    private Neo4jPoolManager()
    {

    }

    public  void init()
    {
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxTotal(100);
        genericObjectPoolConfig.setMinIdle(50);
        genericObjectPoolConfig.setMaxIdle(50);
//        genericObjectPoolConfig.setMinEvictableIdleTimeMillis(config.getMinEvictableIdleTimeMillis());
//        genericObjectPoolConfig.setSoftMinEvictableIdleTimeMillis(config.getSoftMinEvictableIdleTimeMillis());
//        genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(config.getTimeBetweenEvictionRunsMillis());
        genericObjectPoolConfig.setTestOnBorrow(true);
        neo4jPool = new Neo4jPool(genericObjectPoolConfig,neo4jConfig.getUri(),neo4jConfig.getUsername(),neo4jConfig.getPassword(),"metabase-sync");
    }

    public Neo4jSession getNeo4jSession()
    {
        return neo4jPool.getResource();
    }

    public void closeNeo4jSession(Neo4jSession  neo4jSession)
    {
        if (neo4jSession!=null) {
            neo4jSession.close();
        }

    }

    public static  Neo4jPoolManager   getInstance()
    {
        return  Holder.INSTANCE;
    }
    private static  class  Holder
    {
        static final  Neo4jPoolManager  INSTANCE=new Neo4jPoolManager();
    }

    public Neo4jPoolManager setNeo4jConfig(Neo4jConfig neo4jConfig) {
        this.neo4jConfig = neo4jConfig;
        return  this;
    }
}
