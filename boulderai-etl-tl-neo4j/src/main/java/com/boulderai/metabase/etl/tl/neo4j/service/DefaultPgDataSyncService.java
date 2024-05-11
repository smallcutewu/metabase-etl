package com.boulderai.metabase.etl.tl.neo4j.service;

import com.boulderai.metabase.etl.tl.neo4j.config.*;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.INeo4jDataRepository;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepositoryContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.PgWalClient;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.PostgresConnectionManager;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PageInfo;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.util.*;
import com.boulderai.metabase.etl.tl.neo4j.zookeeper.MetaLeaderSelector;
import com.boulderai.metabase.lang.Constants;
import com.boulderai.metabase.lang.er.TableOrder;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @ClassName: DefaultPgDataSyncService
 * @Description: pg wal 后台服务接口具体实现
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */

@Service
@Slf4j
public class DefaultPgDataSyncService   implements IPgDataSyncService {

    private  final static Logger logger = LoggerFactory.getLogger(DefaultPgDataSyncService.class);

    @Autowired
    private PostgresConnectionManager postgresConnectionManager ;

    @Autowired
    private INeo4jDataRepository neo4jDataRepository;

    /**
     * pg wal侦听客户端
     */
    private PgWalClient pgWalClient;

    @Resource
    private SyncPostgresConfig syncPostgresConfig;

    @Autowired
    private PgWalConfig pgWalConfig;

    @Autowired
    private ZookeeperConfig zookeeperConfig ;

    /**
     * 应用状态监听器
     */
    private MetaLeaderSelector appStateWatcher;

    @Autowired
    private Neo4jConfig neo4jConfig;


    @Autowired(required=false)
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitConfig rabbitConfig;
    private static final String  PUBLIC_SCHEMA="public";


    @Value("${spring.profiles.active}")
    private String envName;

    private ScheduledExecutorService walExecutorService;
    private ScheduledExecutorService rabbitMqExecutorService;

//    @Override
//    public void run(String... args) throws Exception {
//        this.init();
//    }

    /**
     * 主要是单元测试报错，增强Unintest
     */
    private void checkRabbitTemplate()
    {
        if (rabbitTemplate==null) {
            CachingConnectionFactory connection = new CachingConnectionFactory();
            connection.setAddresses(rabbitConfig.getHost());
            connection.setUsername(rabbitConfig.getUsername());
            connection.setPassword(rabbitConfig.getPassword());
            connection.setVirtualHost("/");
            rabbitTemplate = new RabbitTemplate(connection);
        }

    }

    /**
     * 等待应用启动完成开始初始化
     */
    public Boolean init()
    {
      File file=new File(DataSynConstant.QUEUE);
      if (!file.exists()) {
          try {
              FileUtils.createParentDirectories(file);
          } catch (Exception e) {
              e.printStackTrace();
          }
      }
        //将本地开发ip的slot名称和开发环境部署slotname区分
        String ip= NetUtils.getLocalAddress();
        if(StringUtils.isNotBlank(ip)&&ip.startsWith(DataSynConstant.IP_LOCAL_DEV_IP))
        {
//            pgWalConfig.setSlotName(DataSynConstant.SLOT_NAME_MATA_DEV);
//            zookeeperConfig.setMasterNodeName(DataSynConstant.ZK_NODE_MASTER_NAME);
            pgWalConfig.setMqQueue(pgWalConfig.getMqQueue() + "_dev");
            pgWalConfig.setMqExchange(pgWalConfig.getMqExchange()+"_dev");
            pgWalConfig.setMqRouting(pgWalConfig.getMqRouting() + "_dev");
        }
        postgresConnectionManager.setPostgresConfig(syncPostgresConfig);
        checkRabbitTemplate();
        pgWalClient=new PgWalClient(pgWalConfig,neo4jConfig,syncPostgresConfig);
        pgWalClient.setRabbitTemplate(rabbitTemplate);
        pgWalClient.setEnvName(envName);
        if(pgWalConfig.getLogRecordCql())
        {
            LogRecordCqlFlag.restart();
        }
        else
        {
            LogRecordCqlFlag.stop();
        }

        /**
         * 目前不需要主从选举服务器，暂时注释掉
         */
//        appStateWatcher =new MetaLeaderSelector(zookeeperConfig);
//        appStateWatcher.registListener(this);
//        appStateWatcher.start();




        onChange(AppCurrentState.MASTER);


        return true;
    }

    /**
     * zk来命令了，我状态发生变化
     * @param state  状态： master slaver
     */
    public void onChange(AppCurrentState state) {
        switch (state) {
            case MASTER:
                onStateChange2Master();
                break;
            default:
//                onStateChange2Slave();
                break;

        }
    }

    /**
     * 我现在是老大，我开始干活le
     */
    private void onStateChange2Master()
    {
        logger.warn(" app  change to master ! begin to start now...... ");
        Stopper.restart();
        postgresConnectionManager.init();
//        neo4jDataRepository.init();
        pgWalClient.start();

        if(walExecutorService ==null)
        {
            walExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "CheckPgWalAlive-pool");
                // 设置线程为守护线程，主线程退出，子线程也随之退出
                t.setDaemon(true);
                return t;
            });
            walExecutorService.scheduleWithFixedDelay(new PgWalAliveChecker(),10,10, TimeUnit.MINUTES);
        }

        if(walExecutorService ==null)
        {
            walExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "CheckPgWalAlive-pool");
                // 设置线程为守护线程，主线程退出，子线程也随之退出
                t.setDaemon(true);
                return t;
            });
            walExecutorService.scheduleWithFixedDelay(new PgWalAliveChecker(),10,10, TimeUnit.MINUTES);
        }

        if(rabbitMqExecutorService ==null)
        {
            rabbitMqExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "RabbitMqTopicAliveCheck-pool");
                // 设置线程为守护线程，主线程退出，子线程也随之退出
                t.setDaemon(true);
                return t;
            });
            rabbitMqExecutorService.scheduleWithFixedDelay(new RabbitMqTopicAliveChecker(),10,10, TimeUnit.MINUTES);
        }
        logger.warn(" app  change to master ! finiah to start now...... ");
    }


    /**
     * 我现在是小弟，我要休息了
     */
    private void onStateChange2Slave()
    {
//        logger.warn(" app  change to slave ! begin to stop now...... ");
//        pgWalClient.close();
//        postgresConnectionManager.init();
//        int count=10;
//        for(int k=0;k<count;k++)
//        {
//            try {
//                Thread.sleep(1000);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            logger.warn("wait for neo4j handle;  app stop step ! "+k+"/"+count);
//        }
////        neo4jDataRepository.close();
//        logger.warn(" app  change to slave ! finish to stop now...... ");
    }


    public void setPostgresConnectionManager(PostgresConnectionManager postgresConnectionManager) {
        this.postgresConnectionManager = postgresConnectionManager;
    }

    public void setNeo4jDataRepository(INeo4jDataRepository neo4jDataRepository) {
        this.neo4jDataRepository = neo4jDataRepository;
    }

    public void setPgWalConfig(PgWalConfig pgWalConfig) {
        this.pgWalConfig = pgWalConfig;
    }

    protected class RabbitMqTopicAliveChecker implements  Runnable {
        @Override
        public void run() {

            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setUsername(rabbitConfig.getUsername());
                factory.setPassword(rabbitConfig.getPassword());
                factory.setVirtualHost("/");
                factory.setHost(rabbitConfig.getHost());
                factory.setPort(rabbitConfig.getPort());

                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                channel.exchangeDeclarePassive(DataSynConstant.MQ_EXCHANGE_FAILMSG_DATA_VALUE);
                channel.queueDeclarePassive(DataSynConstant.MQ_FAILMSG_DATA_QUEUE);
                channel.queueBind(DataSynConstant.MQ_FAILMSG_DATA_QUEUE, DataSynConstant.MQ_EXCHANGE_FAILMSG_DATA_VALUE, DataSynConstant.MQ_ROUTING_FAILMSGI_DATA_KEY);

                log.info("###### Topic queue is available. "+DataSynConstant.MQ_EXCHANGE_FAILMSG_DATA_VALUE+"/"+DataSynConstant.MQ_ROUTING_FAILMSGI_DATA_KEY);
                channel.close();
                connection.close();
            } catch (Exception e) {
               logger.error("Topic queue is not available: " ,e);
                addCqlErrorLog( null,null,3,null, "rabbitmq监控连接错误",null);
            }
        }
    }

    private void addCqlErrorLog(String schema,String tableName,Integer errorType,String cql, String errorDesc,String walChange)
    {
        Object[]  parameters={schema, tableName, errorType, cql,  errorDesc, walChange};
        DbOperateUtil.insert(DataSynConstant.INSERT_CQL_ERROR_LOG_CQL,parameters);
    }


        protected class  PgWalAliveChecker implements  Runnable {
        @Override
        public void run() {
            Boolean  walAlive=false;
            String slotName=""+DataSynConstant.SLOT_NAME_MODEL+"%";
            Map<String, Object>  walMap=   DbOperateUtil.queryMap(DataSynConstant.PG_REPLICATION_SLOTS_SQL,syncPostgresConfig.getDbName(),slotName);
            if(MapUtils.isNotEmpty(walMap))
            {
                Long  walCount= (Long) walMap.get("walCount");
                if(walCount!=null&&walCount==1)
                {
                    walAlive=true;
                }
            }

            if(!walAlive)
            {
                log.info("pg wal not alive! Sys will  start it ......");
                pgWalClient.foreCreateWalStreamHanlde();
            }
            else
            {
                log.info("pg wal check ok!");
            }
        }
    }


    @Override
    public String testWalContext() {
        if(!this.pgWalClient.getPgWalRunning())
        {
            return "pg stream not start !";
        }
        PgWalChange change=new PgWalChange();
        change.setKind("insert");
        change.setTable("testWalContext");
        change.setChangeId("123456");
        Gson gson = new Gson();
        String data=gson.toJson(change);
        RabbitTemplate  rabbitTemplate= SpringApplicationContext.getBean(RabbitTemplate.class);
        try{
            rabbitTemplate.convertAndSend( DataSynConstant.MQ_EXCHANGE_VALUE,  DataSynConstant.MQ_ROUTING_KEY, data);
        }
        catch (Exception ex)
        {
            return "rabbit mq error! "+ex.getMessage();
        }

        return "all ok! Go home!";
    }

    @Override
    public String createNewSlot() {
        return pgWalClient.createNewSlot();
    }

    @Override
    public Boolean dropDisableSlot() {
        return pgWalClient.dropDisableSlot();
    }

    @Override
    public String showWalConfig() {
        StringBuilder  sb=new StringBuilder();
        sb.append(pgWalConfig);
        sb.append("##########################/r/n");
        sb.append(syncPostgresConfig);
        return sb.toString();
    }

    @Override
    public Boolean refreshMetabaseData2Neo4j(String schema, String tableName) {
        String cql="match  (t:"+tableName+") detach delete t ";
        Neo4jDataRepositoryContext.getDefaultNeo4jDataRepository().operNeo4jDataByCqlSingleNoEx(cql,true);
        SleepUtil.sleepSecond(2);
        return true;
    }

    @Override
    public Boolean changeDebug(Boolean debugFlag) {
        this.pgWalClient.changeDebug( debugFlag);
        return debugFlag;
    }

    @Override
    public Boolean refreshData2Neo4j(String schema,  String tableName,int dropNeo4jTableFlag) {
        if (!PUBLIC_SCHEMA.equals(schema)&&dropNeo4jTableFlag==1) {
            Boolean findErrorTable=false;
            for (TableOrder tableOrder: TableOrder.values())
            {
                if (tableOrder.getTableName().equals(tableName)) {
                    findErrorTable=true;
                    break;
                }
            }
            if (!findErrorTable) {
              return  refreshMetabaseData2Neo4j( schema,   tableName);
            }
            return false;
        }
        Map<String,Object> valuesMap = new HashMap<String,Object>(6);
        valuesMap.put("schemaName",schema);
        valuesMap.put("tableName",tableName);
        valuesMap.put("columnName","updated_at");
        valuesMap.put("columnValue","now()");
        StringSubstitutor sub = new StringSubstitutor(valuesMap);
        String  resCql= sub.replace(Constants.UPDATE_TABLE_TIME_SQL);
        int count=DbOperateUtil.update(resCql);
        logger.info(" 一键刷新数据表： "+resCql);
        valuesMap.clear();
        SleepUtil.sleepSecond(2);
        return true;
    }

    public PageInfo<Map> queryTablePage() {

        return null;
    }
}
