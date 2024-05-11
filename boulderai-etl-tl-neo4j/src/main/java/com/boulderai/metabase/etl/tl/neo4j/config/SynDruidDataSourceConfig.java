//package com.boulderai.metabase.sync.core.config;
//
//import com.alibaba.druid.pool.DruidDataSource;
//import com.alibaba.druid.support.http.StatViewServlet;
//import com.alibaba.druid.support.http.WebStatFilter;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.StringUtils;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
//import org.springframework.boot.context.properties.EnableConfigurationProperties;
//import org.springframework.boot.web.servlet.FilterRegistrationBean;
//import org.springframework.boot.web.servlet.ServletRegistrationBean;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
//import javax.servlet.Filter;
//import javax.servlet.Servlet;
//import javax.sql.DataSource;
//import java.sql.SQLException;
//import java.util.Properties;
//
///**
// * @ClassName: SynDruidDataSourceConfig
// * @Description:待监控的数据库连接库池
// * @author  df.l
// * @date 2022年12月09日
// * @Copyright boulderaitech.com
// */
//@Slf4j
//@Configuration
//@EnableConfigurationProperties({SyncPostgresConfig.class})
//public class SynDruidDataSourceConfig {
//    @Autowired
//    private SyncPostgresConfig syncPostgresConfig;
//
//    @Bean
//    @ConditionalOnMissingBean
//    public DruidDataSource druidDataSource() {
//        DruidDataSource druidDataSource = new DruidDataSource();
//        druidDataSource.setDriverClassName(syncPostgresConfig.getDriverClassName());
//        druidDataSource.setUrl(syncPostgresConfig.getUrl());
//        druidDataSource.setUsername(syncPostgresConfig.getUsername());
//        druidDataSource.setPassword(syncPostgresConfig.getPassword());
//        druidDataSource.setInitialSize(syncPostgresConfig.getInitialSize());
//        druidDataSource.setMinIdle(syncPostgresConfig.getMinIdle());
//        druidDataSource.setMaxActive(syncPostgresConfig.getMaxActive());
//        druidDataSource.setMaxWait(syncPostgresConfig.getMaxWait());
//        druidDataSource.setTimeBetweenEvictionRunsMillis(syncPostgresConfig.getTimeBetweenEvictionRunsMillis());
//        druidDataSource.setMinEvictableIdleTimeMillis(syncPostgresConfig.getMinEvictableIdleTimeMillis());
//        druidDataSource.setValidationQuery(syncPostgresConfig.getValidationQuery());
//        druidDataSource.setTestWhileIdle(syncPostgresConfig.getTestWhileIdle());
//        druidDataSource.setTestOnBorrow(syncPostgresConfig.getTestOnBorrow());
//        druidDataSource.setTestOnReturn(syncPostgresConfig.getTestOnReturn());
//        druidDataSource.setPoolPreparedStatements(syncPostgresConfig.getPoolPreparedStatements());
//        druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(syncPostgresConfig.getMaxPoolPreparedStatementPerConnectionSize());
//        if (StringUtils.isNotBlank(syncPostgresConfig.getConnectionProperties())) {
//            Properties property=druidDataSource.getConnectProperties();
//            if(property==null)
//            {
//                property=new Properties();
//                druidDataSource.setConnectProperties(property);
//            }
//
//            String[] connPros =syncPostgresConfig.getConnectionProperties().split(";");
//            if(connPros.length>1)
//            {
//                for (int i = 0; i < connPros.length; i++) {
//                    String[]  kvs=connPros[i].split("=");
//                    if(kvs.length > 1)
//                    {
//                        property.put(kvs[0],kvs[1]);
//                    }
//                }
//            }
//
//        }
//
//        try {
//            druidDataSource.setFilters(syncPostgresConfig.getFilters());
//            druidDataSource.init();
//            log.info("init dataSource  ok !");
//        } catch (Exception e) {
//           log.error("druidDataSource.init()  error!",e);
//        }
//
//        return druidDataSource;
//    }
//
//    /**
//     * 注册Servlet信息， 配置监控视图
//     * @return
//     */
//    @Bean
//    @ConditionalOnMissingBean
//    public ServletRegistrationBean<Servlet> druidServlet() {
//        ServletRegistrationBean<Servlet> servletRegistrationBean = new ServletRegistrationBean<Servlet>(new StatViewServlet(), "/druid/*");
//
//        /**
//         *  druid:
//         *       stat-view-servlet:
//         *         # 默认true 内置监控页面首页/druid/index.html
//         *         enabled: true
//         *         url-pattern: /druid/*
//         *         # 允许清空统计数据
//         *         reset-enable: true
//         *         # 这里为登录页面账号密码配置
//         *         login-username: root
//         *         login-password: 123456
//         *         # IP白名单 多个逗号分隔
//         *         allow:
//         *         # IP黑名单
//         *         deny:
//         *       filter:
//         *         stat:
//         *           # 开启监控sql
//         *           enabled: true
//         *           # 显示并标注慢sql 默认当超过3秒显示
//         *           log-slow-sql: true
//         *           slow-sql-millis: 3000
//         *           merge-sql: true
//         *         # 防SQL注入过滤
//         *         wall:
//         *           config:
//         *             # 允许多条sql同时执行
//         *             multi-statement-allow: true
//         *
//         */
////         servletRegistrationBean.addInitParameter("allow","0.0.0.0");
//        //IP黑名单 (存在共同时，deny优先于allow) : 如果满足deny的话提示:Sorry, you are not permitted to view this page.
////        servletRegistrationBean.addInitParameter("deny","192.168.1.119");
//        //登录查看信息的账号密码, 用于登录Druid监控后台
//        servletRegistrationBean.addInitParameter("loginUsername", "admin");
//        servletRegistrationBean.addInitParameter("loginPassword", "admin654321");
//        //是否能够重置数据.
//        servletRegistrationBean.addInitParameter("resetEnable", "true");
//        return servletRegistrationBean;
//    }
//
//    /**
//     * 注册Filter信息, 监控拦截器
//     * @return
//     */
//    @Bean
//    @ConditionalOnMissingBean
//    public FilterRegistrationBean<Filter> filterRegistrationBean() {
//        FilterRegistrationBean<Filter> filterRegistrationBean = new FilterRegistrationBean<Filter>();
//        filterRegistrationBean.setFilter(new WebStatFilter());
//        filterRegistrationBean.addUrlPatterns("/*");
//        filterRegistrationBean.addInitParameter("exclusions", "*.js,*.gif,*.jpg,*.png,*.css,*.ico,/druid/*");
//        return filterRegistrationBean;
//    }
//}