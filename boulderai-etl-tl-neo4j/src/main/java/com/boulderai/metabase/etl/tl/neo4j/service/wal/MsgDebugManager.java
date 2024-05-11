package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MsgDebugManager {

    private  volatile Boolean  emptyConsumeMetaData=false;
    private  volatile Boolean  emptyConsumeModelData=false;

    private MsgDebugManager()
    {

    }

    public static MsgDebugManager  getInstance() {
        return Holder.INSTANCE;
    }

    public Boolean getEmptyConsumeMetaData() {
        return emptyConsumeMetaData;
    }

    public void setEmptyConsumeMetaData(Boolean emptyConsumeMetaData) {
        this.emptyConsumeMetaData = emptyConsumeMetaData;
        if(this.emptyConsumeMetaData)
        {
            log.info("打开元数据消息消费空转模式......");
        }
        else
        {
            log.info("关闭元数据消息消费空转模式......");
        }
    }

    public Boolean getEmptyConsumeModelData() {
        return emptyConsumeModelData;
    }

    public void setEmptyConsumeModelData(Boolean emptyConsumeModelData) {
        this.emptyConsumeModelData = emptyConsumeModelData;
        if(this.emptyConsumeModelData)
        {
            log.info("打开模型数据消息消费空转模式#######");
        }
        else
        {
            log.info("关闭模型数据消息消费空转模式#######");
        }
    }

    private  static  class Holder
    {
        private  static final MsgDebugManager INSTANCE=new MsgDebugManager();
    }
}
