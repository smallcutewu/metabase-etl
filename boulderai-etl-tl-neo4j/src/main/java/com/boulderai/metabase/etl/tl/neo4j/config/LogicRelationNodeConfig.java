package com.boulderai.metabase.etl.tl.neo4j.config;

import lombok.Data;

public class LogicRelationNodeConfig {
    private  String  startAttrId;
    private  String   endAttrId;
    private  String   startAttrCode;
    private  String   endAttrCode;

    public String getStartAttrId() {
        return startAttrId;
    }

    public void setStartAttrId(String startAttrId) {
        this.startAttrId = startAttrId;
    }

    public String getEndAttrId() {
        return endAttrId;
    }

    public void setEndAttrId(String endAttrId) {
        this.endAttrId = endAttrId;
    }

    public String getStartAttrCode() {
        return startAttrCode;
    }

    public void setStartAttrCode(String startAttrCode) {
        this.startAttrCode = startAttrCode;
    }

    public String getEndAttrCode() {
        return endAttrCode;
    }

    public void setEndAttrCode(String endAttrCode) {
        this.endAttrCode = endAttrCode;
    }

    @Override
    public String toString() {
        return "LogicRelationNodeConfig{" +
                "startAttrId='" + startAttrId + '\'' +
                ", endAttrId='" + endAttrId + '\'' +
                ", startAttrCode='" + startAttrCode + '\'' +
                ", endAttrCode='" + endAttrCode + '\'' +
                '}';
    }
}
