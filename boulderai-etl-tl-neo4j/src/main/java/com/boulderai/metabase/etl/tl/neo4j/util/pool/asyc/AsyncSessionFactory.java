package com.boulderai.metabase.etl.tl.neo4j.util.pool.asyc;


import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.neo4j.driver.*;
import org.neo4j.driver.async.AsyncSession;

import java.util.concurrent.TimeUnit;

public class AsyncSessionFactory implements PooledObjectFactory<AsyncSession> {
    private final Driver driver;
    private final SessionConfig sessionConfig;

    public AsyncSessionFactory(String uri, String userName, String password) {
//        driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));

        Config config = Config.builder()
//                .withMaxConnectionLifetime(30, TimeUnit.MINUTES)
//                .withMaxConnectionPoolSize(50)
//                .withConnectionAcquisitionTimeout(2, TimeUnit.MINUTES)
                .withConnectionLivenessCheckTimeout( 60000, TimeUnit.MILLISECONDS )
                .build();
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(userName, password),config);

        sessionConfig = SessionConfig.builder().withDefaultAccessMode(AccessMode.WRITE).build();
    }

    @Override
    public PooledObject<AsyncSession> makeObject() throws Exception {
        return new DefaultPooledObject<>(driver.asyncSession(sessionConfig));
    }

    @Override
    public void destroyObject(PooledObject<AsyncSession> pooledObject) throws Exception {
        pooledObject.getObject().closeAsync().toCompletableFuture().get();
    }

    @Override
    public boolean validateObject(PooledObject<AsyncSession> pooledObject) {
//        AsyncSession  session=  (AsyncSession)(pooledObject.getObject());
//        return session.isOpen();
        return true;
    }

    @Override
    public void activateObject(PooledObject<AsyncSession> pooledObject) throws Exception {
        // 激活对象时无需执行额外操作
    }

    @Override
    public void passivateObject(PooledObject<AsyncSession> pooledObject) throws Exception {
        // 重置对象状态时无需执行额外操作
    }
}