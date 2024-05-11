package com.boulderai.metabase.etl.tl.neo4j.util;

/**
 * @ClassName: Neo4jPersistException
 * @Description: neo4j持久化异常
 * @author  df.l
 * @date 2022年10月26日
 * @Copyright boulderaitech.com
 */
public class Neo4jPersistException extends RuntimeException{

    private String cql;
    private Object  parameters;

    public Neo4jPersistException(String cql,Object  parameters,Exception ex)
    {
        super(ex);
        this.cql=cql;
        this.parameters=parameters;
    }

    public Neo4jPersistException(String cql,Exception ex)
    {
        super(ex);
        this.cql=cql;
    }

    public String getCql() {
        return cql;
    }

    public void setCql(String cql) {
        this.cql = cql;
    }

    public Object getParameters() {
        return parameters;
    }

    public void setParameters(Object parameters) {
        this.parameters = parameters;
    }
}
