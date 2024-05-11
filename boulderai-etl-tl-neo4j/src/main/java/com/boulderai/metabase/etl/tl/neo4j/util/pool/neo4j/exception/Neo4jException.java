package com.boulderai.metabase.etl.tl.neo4j.util.pool.neo4j.exception;

/**
 * @ClassName: Neo4jException
 * @Description: neo4j通常异常
 * @author  df.l
 * @date 2023年02月11日
 * @Copyright boulderaitech.com
 */
public class Neo4jException extends RuntimeException {
    private static final long serialVersionUID = 3360535414048529387L;

    public Neo4jException(String message) {
        super(message);
    }

    public Neo4jException(Throwable e) {
        super(e);
    }

    public Neo4jException(String message, Throwable cause) {
        super(message, cause);
    }
}

