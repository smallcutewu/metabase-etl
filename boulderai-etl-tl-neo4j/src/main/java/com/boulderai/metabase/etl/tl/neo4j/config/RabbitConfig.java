package com.boulderai.metabase.etl.tl.neo4j.config;

import com.boulderai.metabase.context.env.DefaultEnvironment;
import com.boulderai.metabase.context.env.EnvironmentDefinite;
import com.boulderai.metabase.lang.Constants;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Slf4j
@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "spring.rabbitmq")
@ToString(callSuper = true)
public class RabbitConfig implements InitializingBean {
    private String   host= Constants.UNIT_TEST_RABBITMQ_HOST;
    private Integer     port= Constants.UNIT_TEST_RABBITMQ_PORT;
    private String     username= Constants.UNIT_TEST_RABBITMQ_USERNAME;
    private String      password= Constants.UNIT_TEST_RABBITMQ_PASSWORD;
    private String      virtualHost= "/";


    @Override
    public void afterPropertiesSet() throws Exception {
        EnvironmentDefinite env= DefaultEnvironment.getCurrentEnvironment();
        if(env!=null&&env.equals(EnvironmentDefinite.unit_test)) {
            this.username= Constants.UNIT_TEST_RABBITMQ_USERNAME;
            this.password= Constants.UNIT_TEST_RABBITMQ_PASSWORD;
            this.host= Constants.UNIT_TEST_RABBITMQ_HOST;
            this.port= Constants.UNIT_TEST_RABBITMQ_PORT;
        }

        log.info("RabbitConfig afterPropertiesSet()==>: "+this.toString());
    }
}
