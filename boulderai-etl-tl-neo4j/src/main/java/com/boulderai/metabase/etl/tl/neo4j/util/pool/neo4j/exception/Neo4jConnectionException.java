package com.boulderai.metabase.etl.tl.neo4j.util.pool.neo4j.exception;

/**
 * @ClassName: Neo4jConnectionException
 * @Description: neo4j连接异常
 * @author  df.l
 * @date 2023年02月11日
 * @Copyright boulderaitech.com
 */
public class Neo4jConnectionException extends Neo4jException {
    private static final long serialVersionUID = -2946266495682282677L;

    public Neo4jConnectionException(String message) {
        super(message);
    }

    public Neo4jConnectionException(Throwable e) {
        super(e);
    }

    public Neo4jConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}

