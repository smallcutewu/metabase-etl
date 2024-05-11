package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.alibaba.druid.pool.DruidDataSource;
import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;

@RunWith(MockitoJUnitRunner.Silent.class)
public class PostgresConnectionManagerTest extends SpringBaseTest {

    @Test
    public void testPostgresConnectionManager()
    {
        PostgresConnectionManager  pgManager=new PostgresConnectionManager();
        pgManager.setPostgresConfig(postgresConfig);
        pgManager.init();
        DruidDataSource druidDataSource=pgManager.getDruidDataSource();
        Assert.assertNotNull(druidDataSource);
        Connection connection=pgManager.newNormalConnection();
        Assert.assertNotNull(connection);
        pgManager.onStop();

    }
}
