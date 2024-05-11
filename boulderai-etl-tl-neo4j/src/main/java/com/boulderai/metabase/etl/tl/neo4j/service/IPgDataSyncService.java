package com.boulderai.metabase.etl.tl.neo4j.service;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PageInfo;

import java.util.Map;

/**
 * @ClassName: IPgDataSyncService
 * @Description: pg wal 后台服务接口定义
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public interface IPgDataSyncService {
    public  String testWalContext();
    public String  createNewSlot();
    public Boolean  dropDisableSlot();
    public String  showWalConfig();
    public Boolean  refreshData2Neo4j(String schema,  String tableName,int dropNeo4jTableFlag);
    public Boolean refreshMetabaseData2Neo4j(String schema,  String tableName);

    public Boolean changeDebug(Boolean debugFlag);

    public PageInfo<Map> queryTablePage() ;
}
