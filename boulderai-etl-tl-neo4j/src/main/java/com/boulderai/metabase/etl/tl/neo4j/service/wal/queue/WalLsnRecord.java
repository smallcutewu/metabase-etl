package com.boulderai.metabase.etl.tl.neo4j.service.wal.queue;

import java.sql.Timestamp ;

public class WalLsnRecord {
    private Integer id;
    private String slotName;
    private java.sql.Timestamp   updateTime;
    private String   lastReceiveLsn;
    private Boolean  change=false;

    public WalLsnRecord(Integer id, String slotName) {
        this.id = id;
        this.slotName = slotName;
        this.updateTime=new java.sql.Timestamp (new java.util.Date().getTime());
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getSlotName() {
        return slotName;
    }

    public void setSlotName(String slotName) {
        this.slotName = slotName;
    }

    public java.sql.Timestamp  getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(java.sql.Timestamp  updateTime) {
        this.updateTime = updateTime;
    }

    public String getLastReceiveLsn() {
        change=false;
        return lastReceiveLsn;
    }

    public void setLastReceiveLsn(String lastReceiveLsn) {
        this.lastReceiveLsn = lastReceiveLsn;
        this.updateTime=new Timestamp (new java.util.Date().getTime());
        change=true;
    }

    public Boolean getChange() {
        return change;
    }

    public void setChange(Boolean change) {
        this.change = change;
    }
}
