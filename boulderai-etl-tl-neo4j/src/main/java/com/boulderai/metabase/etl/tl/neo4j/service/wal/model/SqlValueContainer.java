package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class SqlValueContainer {
    private String sql;
    private  List<Object[]> parameterList;

    public SqlValueContainer(String sql) {
        this.sql = sql;
        this.parameterList = new LinkedList<Object[]>();
    }

    public void add(Object[]  params)
    {
        parameterList.add(params);
    }

    public void clear()
    {
        sql=null;
        parameterList.clear();
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<Object[]> getParameterList() {
        return parameterList;
    }

    public void setParameterList(List<Object[]> parameterList) {
        this.parameterList = parameterList;
    }
}
