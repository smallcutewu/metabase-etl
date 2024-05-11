package com.boulderai.metabase.etl.tl.neo4j.util.exception;

import lombok.Getter;

/**
 * @author HanMinyang
 * @description
 * @date 2023/7/13
 */
public class SqlQueryException extends RuntimeException {

    @Getter
    private String sql;
    @Getter
    private Object[] parameters;

    public SqlQueryException(String sql, Object... parameters) {
        this.sql = sql;
        this.parameters = parameters;
    }

    public SqlQueryException(String sql, Exception ex) {
        super(ex);
        this.sql = sql;
    }


}
