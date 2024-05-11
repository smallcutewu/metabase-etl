package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.alibaba.druid.pool.DruidDataSource;
import com.boulderai.metabase.etl.tl.neo4j.config.SyncPostgresConfig;
import org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;


/**
 * @ClassName: PostgresConnectionManager
 * @Description: pg数据库连接管理器
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
@Component
public class PostgresConnectionManager {

    private  final static Logger logger = LoggerFactory.getLogger(PostgresConnectionManager.class);

    private SyncPostgresConfig postgresConfig;

    /**
     * 连接池
     */
//    private DruidDataSource   druidDataSource;

    /**
     * 主从复制连接
     */
//    private    Connection repConnection;

    /**
     * 主从复制连接属性
     */
    private Properties  repProperties;

//    private Boolean  initConnectionRes=false;


    private DruidDataSource druidDataSource ;

    private void checkLocalConfig()
    {
        /**
         * 构造默认连接池
         */
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setName(postgresConfig.getDbName());
        dataSource.setDriverClassName(postgresConfig.getDriverClassName());//如果不配置druid会根据url自动识别dbType，然后选择相应的driverClassName
        dataSource.setUrl(postgresConfig.getUrl());
        dataSource.setUsername(postgresConfig.getUsername());
        dataSource.setPassword(postgresConfig.getPassword());


        dataSource.setMaxActive(postgresConfig.getMaxActive());
        dataSource.setInitialSize(postgresConfig.getInitialSize());
        // 配置获取连接等待超时的时间
        dataSource.setMaxWait(postgresConfig.getMaxWait());
        dataSource.setMinIdle(postgresConfig.getMinIdle());
        // 配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        // 配置一个连接在池中最小生存的时间，单位是毫秒 超过这个时间每次会回收默认3个连接
        dataSource.setMinEvictableIdleTimeMillis(30000);
        // 线上配置的mysql断开闲置连接时间为1小时,数据源配置回收时间为3分钟,以最后一次活跃时间开始算
        dataSource.setMaxEvictableIdleTimeMillis(180000);
        // 连接最大存活时间，默认是-1(不限制物理连接时间)，从创建连接开始计算，如果超过该时间，则会被清理
        dataSource.setPhyTimeoutMillis(15000);
        dataSource.setTestWhileIdle(postgresConfig.getTestWhileIdle());
        dataSource.setTestOnBorrow(postgresConfig.getTestOnBorrow());
        dataSource.setTestOnReturn(postgresConfig.getTestOnReturn());
        dataSource.setPoolPreparedStatements(postgresConfig.getPoolPreparedStatements());
        dataSource.setMaxOpenPreparedStatements(30);
        dataSource.setUseGlobalDataSourceStat(true);
        dataSource.setKeepAlive(true);
        dataSource.setRemoveAbandoned(true);
        dataSource.setRemoveAbandonedTimeout(180);
        /**
         * //用来检测连接是否有效
         */
        dataSource.setValidationQuery(" select 1  ");

        /**
         * 初始化连接池
         */
        try
        {
            dataSource.init();
            this.druidDataSource = dataSource;
            logger.info("init dataSource  ok !");
        }
        catch (Exception ex)
        {
            logger.error("init dataSource  error!",ex);
        }
    }

    public void init ()
    {
       if( druidDataSource ==null)
       {
           this.checkLocalConfig();
       }


        initRepConnProper();

        if( this.druidDataSource != null )
        {
           // Connection    repConnection = newRepConnection() ;
//            if(repConnection!=null)
//            {
//                initConnectionRes=true;
//            }
        }
    }

    public void onStop()
    {

//        initConnectionRes=false;
        closedDataSource();
    }

    private void closedDataSource()
    {
        if(druidDataSource!=null&&!druidDataSource.isClosed())
        {
            druidDataSource.close();
        }
        druidDataSource=null;
    }




    /**
     * 初始化主从复制连接属性
     */
    private void initRepConnProper()
    {
        Properties props = new Properties();
        PGProperty.USER.set(props, postgresConfig.getUsername());
        PGProperty.PASSWORD.set(props, postgresConfig.getPassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

//        props.setProperty("include.schema.changes", "true");
//        props.setProperty("snapshot.mode", "initial");
//        props.setProperty("publication.autocreate.mode", "filtered");
//        props.setProperty("include.schema.changes", "true");
//        禁止快照
        props.setProperty("snapshot.mode", "never");
//        props.setProperty("decimal.handling.mode", "double"); //debezium 小数转换处理策略
//        props.setProperty("database.serverTimezone", "GMT+8"); //debezium 配置以database. 开头的属性将被传递给jdbc url

        this.repProperties=props;

    }

//    public Boolean  isInitConnectionRes()
//    {
//        return this.initConnectionRes;
//    }

    /**
     * 创建主从复制连接
     * @return
     */
    public Connection  newRepConnection()
    {
        Connection  conn= null;
        try {
            conn = DriverManager.getConnection(postgresConfig.getUrl(), repProperties);
        } catch (Exception ex) {
            logger.error("newRepConnection  from  dataSource  error!",ex);
        }
        return conn;
    }

    /**
     * 创建普通的连接
     * @return
     */
    public Connection  newNormalConnection()
    {
        Connection  conn= null;
        try {
            conn = druidDataSource.getConnection();
        } catch (Exception ex) {
            logger.error("newNormalConnection  from  dataSource  error!",ex);
        }
        return conn;
    }

//    public  Connection  getRepConnection()
//    {
//        try {
//            if(this.repConnection==null||this.repConnection.isClosed())
//            {
//                this.repConnection=newRepConnection();
//            }
//        } catch (SQLException ex) {
//            logger.error("newRepConnection  error!",ex);
//        }
//        return  this.repConnection;
//    }

    public void closeDataSource() {
        if (druidDataSource != null && !druidDataSource.isClosed()) {
            druidDataSource.close();
            druidDataSource = null;
        }
    }

    public  DruidDataSource  getDruidDataSource()
    {
        return  this.druidDataSource;
    }

    public void setPostgresConfig(SyncPostgresConfig postgresConfig) {
        this.postgresConfig = postgresConfig;
    }
}
