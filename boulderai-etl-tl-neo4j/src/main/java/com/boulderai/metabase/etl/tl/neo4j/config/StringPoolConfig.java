package com.boulderai.metabase.etl.tl.neo4j.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@EnableConfigurationProperties
@ConfigurationProperties("strpool")
@Data
public class StringPoolConfig {
    private Integer  maxTotal=100;
    private Integer  minIdle=10;
    private Integer  maxIdle=50;
    private Integer minEvictableIdleTimeMillis=5000;
    private Integer  softMinEvictableIdleTimeMillis=10000;
    private Integer timeBetweenEvictionRunsMillis=1000;
    private Boolean  testOnBorrow=true;

}
