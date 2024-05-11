package com.boulderai.metabase.etl.tl.neo4j.config;

import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @ClassName: ZookeeperConfig
 * @Description: zk配置
 * @author  df.l
 * @date 2022年09月07日
 * @Copyright boulderaitech.com
 */

@Data
@Component
@EnableConfigurationProperties
@ConfigurationProperties("zookeeper")
public class ZookeeperConfig {


    /**
     * zk地址
     */
    private String zookeeperUrl;

    /**
     * 主从注册节点路径名称
     */
    private String masterNodeName;

    /**
     * 连接超时时间
     */
    private Integer  connectionTimeout=6000;

    /**
     * session 超时时间
     */
    private  Integer sessionTimeout=6000;


}
