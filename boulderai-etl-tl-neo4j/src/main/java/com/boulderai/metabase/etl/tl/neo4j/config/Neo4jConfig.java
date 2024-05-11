package com.boulderai.metabase.etl.tl.neo4j.config;

import com.boulderai.metabase.context.env.DefaultEnvironment;
import com.boulderai.metabase.context.env.EnvironmentDefinite;
import com.boulderai.metabase.lang.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;


/**
 * @ClassName: Neo4jConfig
 * @Description: Neo4j配置，暂时使用文件配置取代nacos
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */

@Component
@EnableConfigurationProperties
@ConfigurationProperties("neo4jconfig")
@Slf4j
public class Neo4jConfig  implements InitializingBean {
    private String username= Constants.UNIT_TEST_NEO4J_CONFIG_USERNAME;
    private String password= Constants.UNIT_TEST_NEO4J_CONFIG_PASSWORD;;
    private String uri= Constants.UNIT_TEST_NEO4J_CONFIG_URI;
    private String autoIndex;

    private int  poolSize=150;

    private int integraThreadCount=1;
    private int eimosThreadCount=3;

    private Boolean  debugLog=true;

    public Boolean getDebugLog() {
        return debugLog;
    }

    public void setDebugLog(Boolean debugLog) {
        this.debugLog = debugLog;
    }

    public int getIntegraThreadCount() {
        return integraThreadCount;
    }

    public void setIntegraThreadCount(int integraThreadCount) {
        this.integraThreadCount = integraThreadCount;
    }

    public int getEimosThreadCount() {
        return eimosThreadCount;
    }

    public void setEimosThreadCount(int eimosThreadCount) {
        this.eimosThreadCount = eimosThreadCount;
    }

    private Boolean  createConstraint =false;

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public Boolean getCreateConstraint() {
        return createConstraint;
    }

    public void setCreateConstraint(Boolean createConstraint) {
        this.createConstraint = createConstraint;
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

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getAutoIndex() {
        return autoIndex;
    }

    public void setAutoIndex(String autoIndex) {
        this.autoIndex = autoIndex;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        EnvironmentDefinite env= DefaultEnvironment.getCurrentEnvironment();
        if(env!=null&&env.equals(EnvironmentDefinite.unit_test))
        {
            this.username= Constants.UNIT_TEST_NEO4J_CONFIG_USERNAME;
            this.password= Constants.UNIT_TEST_NEO4J_CONFIG_PASSWORD;
            this.uri= Constants.UNIT_TEST_NEO4J_CONFIG_URI;
        }

        log.info("ENV: ;"+env.toString()+"Neo4jConfig afterPropertiesSet()==>: "+this.toString());
    }

    @Override
    public String toString() {
        return "Neo4jConfig{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", uri='" + uri + '\'' +
                ", autoIndex='" + autoIndex + '\'' +
                ", createConstraint=" + createConstraint +
                ", integraThreadCount"+ integraThreadCount +
                ", eimosThreadCount"+ eimosThreadCount +
                '}';
    }
}
