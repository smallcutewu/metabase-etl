package com.boulderai.metabase.etl.tl.neo4j.config;

import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @ClassName: PgWalConfig
 * @Description: PgWal配置，暂时使用文件配置取代nacos
 * @author  df.l
 * @date 2022年10月16日
 * @Copyright boulderaitech.com
 */
@Component
@EnableConfigurationProperties
@ConfigurationProperties("pgwal")
public class PgWalConfig {

    /**
     * pg pipe分组启动的线程个数:元数据
     */
    private Integer  pipeWorkCount=10;

    /**
     * pg pipe分组启动的线程个数:模型数据
     */
    private Integer  modelDataPipeWorkCount=64;

    /**
     * wal slot 名称
     */
    private String   slotName= "metabase_slot";

    /**
     * 等待时间 wal stream时间
     */
    private Integer   waitMin= 200 ;

    /**
     * wal读取超时时间
     */
    private Integer   walReadTimeout= 20000 ;

    /**
     * 单次获取的Binlog最大批大小，单位为行，默认值为1024。
     */
    private Integer  walBatchSize=1024;

    /**
     * zk地址
     */
//    private String zookeeperUrl;

    /**
     * 主从注册节点路径名称
     */
    private String masterNodeName;

    /**
     * 连接超时时间
     */
    private Integer  zkTimeoutSessionMiliseconds=6000;

    private String  excludeTables="sc_warehouse_period_snapshot";


    private String mqQueue="queue.metabase.wal";
    private String mqExchange="exchange.metabase.wal";
    private String mqRouting="routing.metabase.wal";
    private Boolean  notifyOnDataError=false;
    private Boolean  useNewRelation=false;
    private Boolean  walFilterTable=false;
    private String walSubscribeSchema=  DataSynConstant.WAL_SUBSCRIBE_SCHEMA_LIST;
    private  Boolean  clearingNeo4j=false;
    private  Boolean  clearAllNeo4jOnStart=false;
    private Boolean  saveIntegrationData=true;

    private  Boolean   fieldDataAvailabilityCanUpdate=false;

    private Boolean  checkPgNeo4jRecCount=true;

    private Integer  maxNeo4jDelayPercent=2;

    private Integer  maxNeo4jDelayCount=1000;

    private  String    specIndexList =" index_b5b691dd ";


    private  Boolean logRecordCql=false;
    private  Boolean showDataLog=true;

    private Boolean  dingdingNotify=true;

    private  String  realProjectName="dev";

    private Boolean clusterSwitch = true;

    private Boolean deleteModelfilter = false;

    public Boolean getDeleteModelfilter() {
        return deleteModelfilter;
    }

    public void setDeleteModelfilter(Boolean deleteModelfilter) {
        this.deleteModelfilter = deleteModelfilter;
    }

    public Boolean getClusterSwitch() {
        return clusterSwitch;
    }

    public void setClusterSwitch(boolean clusterSwitch) {
        this.clusterSwitch = clusterSwitch;
    }

    public String getRealProjectName() {
        return realProjectName;
    }

    public void setRealProjectName(String realProjectName) {
        this.realProjectName = realProjectName;
    }

    public Boolean getShowDataLog() {
        return showDataLog;
    }

    public void setShowDataLog(Boolean showDataLog) {
        this.showDataLog = showDataLog;
    }

    public Boolean getDingdingNotify() {
        return dingdingNotify;
    }

    public void setDingdingNotify(Boolean dingdingNotify) {
        this.dingdingNotify = dingdingNotify;
    }

    public Boolean getLogRecordCql() {
        return logRecordCql;
    }

    public void setLogRecordCql(Boolean logRecordCql) {
        this.logRecordCql = logRecordCql;
    }

    public String getSpecIndexList() {
        return specIndexList;
    }

    public void setSpecIndexList(String specIndexList) {
        this.specIndexList = specIndexList;
    }

    public Integer getMaxNeo4jDelayCount() {
        return maxNeo4jDelayCount;
    }

    public void setMaxNeo4jDelayCount(Integer maxNeo4jDelayCount) {
        this.maxNeo4jDelayCount = maxNeo4jDelayCount;
    }

    public Integer getMaxNeo4jDelayPercent() {
        return maxNeo4jDelayPercent;
    }

    public void setMaxNeo4jDelayPercent(Integer maxNeo4jDelayPercent) {
        this.maxNeo4jDelayPercent = maxNeo4jDelayPercent;
    }

    public Boolean getCheckPgNeo4jRecCount() {
        return checkPgNeo4jRecCount;
    }

    public void setCheckPgNeo4jRecCount(Boolean checkPgNeo4jRecCount) {
        this.checkPgNeo4jRecCount = checkPgNeo4jRecCount;
    }

    public Boolean getSaveIntegrationData() {
        return saveIntegrationData;
    }

    public void setSaveIntegrationData(Boolean saveIntegrationData) {
        this.saveIntegrationData = saveIntegrationData;
    }

    public Boolean getFieldDataAvailabilityCanUpdate() {
        return fieldDataAvailabilityCanUpdate;
    }

    public void setFieldDataAvailabilityCanUpdate(Boolean fieldDataAvailabilityCanUpdate) {
        this.fieldDataAvailabilityCanUpdate = fieldDataAvailabilityCanUpdate;
    }

    public Boolean getClearAllNeo4jOnStart() {
        return clearAllNeo4jOnStart;
    }

    public void setClearAllNeo4jOnStart(Boolean clearAllNeo4jOnStart) {
        this.clearAllNeo4jOnStart = clearAllNeo4jOnStart;
    }

    public Boolean getClearingNeo4j() {
        return clearingNeo4j;
    }

    public void setClearingNeo4j(Boolean clearingNeo4j) {
        this.clearingNeo4j = clearingNeo4j;
    }

    public String getWalSubscribeSchema() {
        return walSubscribeSchema;
    }

    public void setWalSubscribeSchema(String walSubscribeSchema) {
        this.walSubscribeSchema = walSubscribeSchema;
    }

    public Boolean getWalFilterTable() {
        return walFilterTable;
    }

    public void setWalFilterTable(Boolean walFilterTable) {
        this.walFilterTable = walFilterTable;
    }

    public String getMqQueue() {
        return mqQueue;
    }

    public void setMqQueue(String mqQueue) {
        this.mqQueue = mqQueue;
    }

    public Integer getPipeWorkCount() {
        return pipeWorkCount;
    }

    public void setPipeWorkCount(Integer pipeWorkCount) {
        this.pipeWorkCount = pipeWorkCount;
    }

    public String getSlotName() {
        return slotName;
    }

    public void setSlotName(String slotName) {
        this.slotName = slotName;
    }

    public Integer getWaitMin() {
        return waitMin;
    }

    public void setWaitMin(Integer waitMin) {
        this.waitMin = waitMin;
    }

    public Integer getWalReadTimeout() {
        return walReadTimeout;
    }

    public void setWalReadTimeout(Integer walReadTimeout) {
        this.walReadTimeout = walReadTimeout;
    }

//    public String getZookeeperUrl() {
//        return zookeeperUrl;
//    }
//
//    public void setZookeeperUrl(String zookeeperUrl) {
//        this.zookeeperUrl = zookeeperUrl;
//    }

    public String getMqExchange() {
        return mqExchange;
    }

    public void setMqExchange(String mqExchange) {
        this.mqExchange = mqExchange;
    }

    public String getMqRouting() {
        return mqRouting;
    }

    public void setMqRouting(String mqRouting) {
        this.mqRouting = mqRouting;
    }

    public String getMasterNodeName() {
        return masterNodeName;
    }

    public void setMasterNodeName(String masterNodeName) {
        this.masterNodeName = masterNodeName;
    }

    public Integer getZkTimeoutSessionMiliseconds() {
        return zkTimeoutSessionMiliseconds;
    }

    public void setZkTimeoutSessionMiliseconds(Integer zkTimeoutSessionMiliseconds) {
        this.zkTimeoutSessionMiliseconds = zkTimeoutSessionMiliseconds;
    }

    public Integer getModelDataPipeWorkCount() {
        return modelDataPipeWorkCount;
    }

    public void setModelDataPipeWorkCount(Integer modelDataPipeWorkCount) {
        this.modelDataPipeWorkCount = modelDataPipeWorkCount;
    }

    public Integer getWalBatchSize() {
        return walBatchSize;
    }

    public void setWalBatchSize(Integer walBatchSize) {
        this.walBatchSize = walBatchSize;
    }

    public Boolean getNotifyOnDataError() {
        return notifyOnDataError;
    }

    public void setNotifyOnDataError(Boolean notifyOnDataError) {
        this.notifyOnDataError = notifyOnDataError;
    }

    public Boolean getUseNewRelation() {
        return useNewRelation;
    }

    public String getExcludeTables() {
        return excludeTables;
    }

    public void setExcludeTables(String excludeTables) {
        this.excludeTables = excludeTables;
    }

    public void setUseNewRelation(Boolean useNewRelation) {
        this.useNewRelation = useNewRelation;
    }
}
