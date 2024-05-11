package com.boulderai.metabase.etl.tl.neo4j;

import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.SyncPostgresConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.ZookeeperConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.DefaultPgDataSyncService;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.INeo4jDataRepository;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.MqMetabaseListenerConsumer;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.MqModelDataListenerConsumer;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.PostgresConnectionManager;
import com.boulderai.metabase.etl.tl.neo4j.util.SpringApplicationContext;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepository;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepositoryMocker;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.amqp.rabbit.connection.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ApplicationContext;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;



/**
 * @ClassName: SpringBaseTest
 * @Description: spring测试基础类
 * @author  df.l
 * @date 2022.11.11
 * @Copyright boulderaitech.com
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class SpringBaseTest   extends BaseTest{
    protected SyncPostgresConfig postgresConfig ;
    protected Neo4jConfig neo4jConfig ;
    protected PgWalConfig pgWalConfig ;
    protected PostgresConnectionManager postgresConnectionManager ;
    protected INeo4jDataRepository neo4jDataRepository ;
    protected DefaultPgDataSyncService defaultPgDataSyncService;
    protected ZookeeperConfig zookeeperConfig;
    protected  ApplicationContext applicationContext;
    protected Boolean  useMockNeo4j=false;


    @Before
    public void before() throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
         applicationContext = Mockito.mock(ApplicationContext.class);
        SpringApplicationContext springApplicationContext = new SpringApplicationContext();
        springApplicationContext.setApplicationContext(applicationContext);

        postgresConfig=createPostgresConfig();
        Mockito.when(applicationContext.getBean(SyncPostgresConfig.class)).thenReturn(postgresConfig);

        neo4jConfig=createNeo4jConfig();
        Mockito.when(applicationContext.getBean(Neo4jConfig.class)).thenReturn(neo4jConfig);

        pgWalConfig=new PgWalConfig();
        Mockito.when(applicationContext.getBean(PgWalConfig.class)).thenReturn(pgWalConfig);

        postgresConnectionManager=new PostgresConnectionManager() ;
        postgresConnectionManager.setPostgresConfig(postgresConfig);
        postgresConnectionManager.init();
        Mockito.when(applicationContext.getBean(PostgresConnectionManager.class)).thenReturn(postgresConnectionManager);

        if (useMockNeo4j) {
            neo4jDataRepository=new Neo4jDataRepositoryMocker();
        }
        else
        {
            neo4jDataRepository=new Neo4jDataRepository();
        }

//        neo4jDataRepository.setNeo4jConfig(neo4jConfig);
        neo4jDataRepository.init();
        Mockito.when(applicationContext.getBean(INeo4jDataRepository.class)).thenReturn(neo4jDataRepository);

        MqMetabaseListenerConsumer mqMetabaseListenerConsumer=new MqMetabaseListenerConsumer();
        mqMetabaseListenerConsumer.setNeo4jConfig(neo4jConfig);
        mqMetabaseListenerConsumer.setPgWalConfig(pgWalConfig);
        Mockito.when(applicationContext.getBean(MqMetabaseListenerConsumer.class)).thenReturn(mqMetabaseListenerConsumer);


        MqModelDataListenerConsumer mqModelDataListenerConsumer=new MqModelDataListenerConsumer();
        mqModelDataListenerConsumer.setNeo4jConfig(neo4jConfig);
        mqModelDataListenerConsumer.setPgWalConfig(pgWalConfig);
        Mockito.when(applicationContext.getBean(MqModelDataListenerConsumer.class)).thenReturn(mqModelDataListenerConsumer);


        defaultPgDataSyncService=new DefaultPgDataSyncService();
        defaultPgDataSyncService.setNeo4jDataRepository(neo4jDataRepository);
        defaultPgDataSyncService.setPgWalConfig(pgWalConfig);
        defaultPgDataSyncService.setPostgresConnectionManager(postgresConnectionManager);
        Mockito.when(applicationContext.getBean(DefaultPgDataSyncService.class)).thenReturn(defaultPgDataSyncService);

        zookeeperConfig=this.createZookeeperConfig();
        Mockito.when(applicationContext.getBean(ZookeeperConfig.class)).thenReturn(zookeeperConfig);

        URI uri =new URI("amqp://admin:admin@172.16.5.68:5672");
        CachingConnectionFactory connectionFactory =new CachingConnectionFactory(uri);
        connectionFactory.getRabbitConnectionFactory().setHandshakeTimeout(20000);

        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        Mockito.when(applicationContext.getBean(RabbitTemplate.class)).thenReturn(rabbitTemplate);
    }

}
