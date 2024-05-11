package com.boulderai.metabase.etl.tl.neo4j.config;


import com.boulderai.metabase.context.env.DefaultEnvironment;
import com.boulderai.metabase.context.env.EnvironmentDefinite;
import com.boulderai.metabase.lang.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: PostgresConfig
 * @Description: Postgres配置，暂时使用文件配置取代nacos
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */

@Slf4j
@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix="spring.datasource.druid.postgresql")
public class SyncPostgresConfig implements InitializingBean {

    private  static  final Pattern DB_NAME_PATTERN = Pattern.compile("/(metabase.*?)\\?");

    /**
     * url链接地址
     */
    private String url= Constants.UNIT_TEST_DB_URL;

    /**
     * 用户名
     */
    private String username= Constants.UNIT_TEST_DB_USERNAME;

    /**
     * 密码
     */
    private String password= Constants.UNIT_TEST_DB_PASSWORD;

    /**
     * driver名称
     */
    private String driverClassName= Constants.UNIT_TEST_DB_DRIVER_CLASS_NAME;

    /**
     * wal db 名称
     */
    private String dbName= Constants.UNIT_TEST_DB_NAME;


    private Integer   initialSize= 2;
    private Integer  minIdle= 2;
    private Integer  maxActive= 10;
    private Integer  maxWait=60000;
    private Integer  timeBetweenEvictionRunsMillis= 60000;
    private Integer  minEvictableIdleTimeMillis= 300000;
    private String   validationQuery= " SELECT '1' ";
    private Boolean   testWhileIdle= true;
    private Boolean   testOnBorrow= false;
    private Boolean    testOnReturn= false;
    private Boolean    poolPreparedStatements=true ;
    private Integer   maxPoolPreparedStatementPerConnectionSize=20;
    private Integer   maxOpenPreparedStatements= 50;
    private String connectionProperties;
    private String filters;

    public String getFilters() {
        return filters;
    }

    public void setFilters(String filters) {
        this.filters = filters;
    }

    public String getConnectionProperties() {
        return connectionProperties;
    }

    public void setConnectionProperties(String connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Integer getInitialSize() {
        return initialSize;
    }

    public void setInitialSize(Integer initialSize) {
        this.initialSize = initialSize;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }



    public Integer getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(Integer minIdle) {
        this.minIdle = minIdle;
    }

    public Integer getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(Integer maxActive) {
        this.maxActive = maxActive;
    }

    public Integer getMaxWait() {
        return maxWait;
    }

    public void setMaxWait(Integer maxWait) {
        this.maxWait = maxWait;
    }

    public Integer getTimeBetweenEvictionRunsMillis() {
        return timeBetweenEvictionRunsMillis;
    }

    public void setTimeBetweenEvictionRunsMillis(Integer timeBetweenEvictionRunsMillis) {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    public Integer getMinEvictableIdleTimeMillis() {
        return minEvictableIdleTimeMillis;
    }

    public void setMinEvictableIdleTimeMillis(Integer minEvictableIdleTimeMillis) {
        this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
    }

    public String getValidationQuery() {
        return validationQuery;
    }

    public void setValidationQuery(String validationQuery) {
        this.validationQuery = validationQuery;
    }

    public Boolean getTestWhileIdle() {
        return testWhileIdle;
    }

    public void setTestWhileIdle(Boolean testWhileIdle) {
        this.testWhileIdle = testWhileIdle;
    }

    public Boolean getTestOnBorrow() {
        return testOnBorrow;
    }

    public void setTestOnBorrow(Boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    public Boolean getTestOnReturn() {
        return testOnReturn;
    }

    public void setTestOnReturn(Boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }

    public Boolean getPoolPreparedStatements() {
        return poolPreparedStatements;
    }

    public void setPoolPreparedStatements(Boolean poolPreparedStatements) {
        this.poolPreparedStatements = poolPreparedStatements;
    }

    public Integer getMaxPoolPreparedStatementPerConnectionSize() {
        return maxPoolPreparedStatementPerConnectionSize;
    }

    public void setMaxPoolPreparedStatementPerConnectionSize(Integer maxPoolPreparedStatementPerConnectionSize) {
        this.maxPoolPreparedStatementPerConnectionSize = maxPoolPreparedStatementPerConnectionSize;
    }

    public Integer getMaxOpenPreparedStatements() {
        return maxOpenPreparedStatements;
    }

    public void setMaxOpenPreparedStatements(Integer maxOpenPreparedStatements) {
        this.maxOpenPreparedStatements = maxOpenPreparedStatements;
    }

    @Override
    public String toString() {
        return "SyncPostgresConfig{" +
                "url='" + url + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", driverClassName='" + driverClassName + '\'' +
                ", dbName='" + dbName + '\'' +
                ", initialSize=" + initialSize +
                ", minIdle=" + minIdle +
                ", maxActive=" + maxActive +
                ", maxWait=" + maxWait +
                ", timeBetweenEvictionRunsMillis=" + timeBetweenEvictionRunsMillis +
                ", minEvictableIdleTimeMillis=" + minEvictableIdleTimeMillis +
                ", validationQuery='" + validationQuery + '\'' +
                ", testWhileIdle=" + testWhileIdle +
                ", testOnBorrow=" + testOnBorrow +
                ", testOnReturn=" + testOnReturn +
                ", poolPreparedStatements=" + poolPreparedStatements +
                ", maxPoolPreparedStatementPerConnectionSize=" + maxPoolPreparedStatementPerConnectionSize +
                ", maxOpenPreparedStatements=" + maxOpenPreparedStatements +
                ", connectionProperties='" + connectionProperties + '\'' +
                ", filters='" + filters + '\'' +
                '}';
    }

    private static  final  String DEV_DB_NAME="metabase";
    private static  final  String TEST_DB_NAME="metabase_unit_test";

    @Override
    public void afterPropertiesSet() throws Exception {
        EnvironmentDefinite env= DefaultEnvironment.getCurrentEnvironment();
        if(env!=null&&env.equals(EnvironmentDefinite.unit_test))
        {
            this.url= Constants.UNIT_TEST_DB_URL;
            this.username= Constants.UNIT_TEST_DB_USERNAME;
            this.password= Constants.UNIT_TEST_DB_PASSWORD;
            this.driverClassName= Constants.UNIT_TEST_DB_DRIVER_CLASS_NAME;
        }

        Matcher  matcher= DB_NAME_PATTERN.matcher(this.url);
        if (matcher.find()) {
            this.dbName=matcher.group(1);
        }


        log.info(env+ " metabase-syn SyncPostgresConfig afterPropertiesSet()==>: "+this.toString());
    }
}
