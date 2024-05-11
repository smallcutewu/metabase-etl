package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;


import org.apache.commons.codec.digest.DigestUtils;
import org.neo4j.driver.*;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Objects;

/**
 * @ClassName: CqlValueContainer
 * @Description: pg wal cql和列值保存其容器
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public class CqlValueContainer {
    /**
     * cql语句
     */
    private  String cql;

    /**
     * 容器id，同一张表同一操作可能有多个容器
     */
    private  String id;

    /**
     * 本次修改每列的值
     */
    private LinkedList<Value>  valueList;

    /**
     * 事件类型
     */
    private EventType  eventType;

    /**
     * 表名
     */
    private  String  tableName;



    public CqlValueContainer() {

    }


    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public CqlValueContainer(String cql,EventType  eventType,String  tableName) {
        this.cql = cql;
        this.eventType = eventType;
        this.tableName = tableName;
        this.valueList=new LinkedList<Value>();
        this.id=CqlValueContainer.makeKey(  eventType,  tableName, cql);
    }

    public  static String makeKey(EventType  eventType,String  tableName,String cql)
    {
        return tableName+"_"+eventType.toString()+"_"+CqlValueContainer.cql2Id(cql);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public  static   String cql2Id(String cql)
    {
        return  DigestUtils.md5Hex(cql.getBytes(StandardCharsets.UTF_8));
    }

    public void addValue(Value  value)
    {
        this.valueList.addLast(value);
    }

    public CqlValueContainer(String cql, LinkedList<Value> valueList,EventType  eventType,String  tableName) {
        this.cql = cql;
        this.valueList = valueList;
        this.eventType = eventType;
        this.tableName = tableName;
        this.id=CqlValueContainer.makeKey(  eventType,  tableName, cql);
    }

    public String getCql() {
        return cql;
    }

    public void setCql(String cql) {
        this.cql = cql;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public LinkedList<Value> getValueList() {
        return valueList;
    }

    public void setValueList(LinkedList<Value> valueList) {
        this.valueList = valueList;
    }

    public void clear()
    {
       this.cql=null;
        this.id=null;
        this.tableName=null;
        if(valueList!=null)
        {
            valueList.clear();
            valueList=null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CqlValueContainer that = (CqlValueContainer) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "CqlValueContainer{" +
                "cql='" + cql + '\'' +
                ", id='" + id + '\'' +
                ", valueList=" + valueList +
                ", eventType=" + eventType +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
