package com.boulderai.metabase.etl.tl.neo4j.util.pool.neo4j;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.neo4j.driver.*;

import java.util.concurrent.TimeUnit;


/**
 * @ClassName: Neo4jSessionFactory
 * @Description: 实际连接的工厂
 * @author  df.l
 * @date 2023年02月11日
 * @Copyright boulderaitech.com
 */
public class Neo4jSessionFactory implements PooledObjectFactory<Neo4jSession> {
    private String uri;
    private String user;
    private String password;
    private String clientName;
    //驱动
    private Driver driver;

    /**
     * 连接属性
     * @param uri
     * @param user
     * @param password
     * @param clientName
     */
    public Neo4jSessionFactory(String uri, String user, String password,  String clientName) {
        this.uri=uri;
        this.user=user;
        this.password=password;
        this.clientName=clientName;
        init();
    }

    private void init(){
        Config config = Config.builder()
//                .withMaxConnectionLifetime(30, TimeUnit.MINUTES)
//                .withMaxConnectionPoolSize(50)
//                .withConnectionAcquisitionTimeout(2, TimeUnit.MINUTES)
              .withConnectionLivenessCheckTimeout( 60000, TimeUnit.MILLISECONDS )
                .build();
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password),config);
    }

    @Override
    public PooledObject<Neo4jSession> makeObject() throws Exception {
        Session session=this.driver.session();
        if (session!=null){
            Neo4jSession neo4jSession= new Neo4jSession();
            neo4jSession.setSession(session);
            return new DefaultPooledObject<>(neo4jSession);
        }
        return null;
    }

    @Override
    public void destroyObject(PooledObject<Neo4jSession> pooledObject) throws Exception {
        final Neo4jSession neo4jSession=pooledObject.getObject();
        //TODO：如果有其他额外资源的话可以处理
    }

    @Override
    public boolean validateObject(PooledObject<Neo4jSession> pooledObject) {
        final Neo4jSession neo4jSession=pooledObject.getObject();
        if (neo4jSession.isOpen()){
            return true;
        }
        return false;
    }

    @Override
    public void activateObject(PooledObject<Neo4jSession> pooledObject) throws Exception {

    }

    @Override
    public void passivateObject(PooledObject<Neo4jSession> pooledObject) throws Exception {

    }
}
