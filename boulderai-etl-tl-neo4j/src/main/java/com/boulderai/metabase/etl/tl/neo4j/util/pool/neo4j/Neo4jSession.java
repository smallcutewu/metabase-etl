package com.boulderai.metabase.etl.tl.neo4j.util.pool.neo4j;

import org.neo4j.driver.Session;


/**
 * @ClassName: Neo4jSession
 * @Description: 连接对象
 * @author  df.l
 * @date 2023年02月11日
 * @Copyright boulderaitech.com
 */
public class Neo4jSession {
    private Neo4jPool dataSource;
    private  Session session;
    private long lastUseTime=0L;

    public void setDataSource(Neo4jPool neo4jPool) {
        this.dataSource=neo4jPool;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public boolean isOpen(){
        return this.session.isOpen();
    }

    public void close(){
        //dataSource.returnBrokenResource(this);   returnBrokenResource是将brock的资源释放掉
        dataSource.returnResource(this);
        lastUseTime=System.currentTimeMillis();
    }
}
