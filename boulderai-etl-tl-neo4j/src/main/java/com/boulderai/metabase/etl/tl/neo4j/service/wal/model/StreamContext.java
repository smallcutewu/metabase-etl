package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.replication.PGReplicationStream;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class StreamContext {
    private PGReplicationStream stream ;
    private PGConnection pgConnection;
    private Connection jdbcConnection;

    public void close()
    {
        if (stream!=null&&!stream.isClosed()) {
            try {
                stream.close();
            } catch (Exception ex) {
                log.error(" stream.close()  error!",ex);
            }

        }
        stream=null;

        if (pgConnection!=null) {
            try {
                pgConnection.cancelQuery();
            } catch (Exception ex) {
                log.error("  pgConnection.cancelQuery()  error!",ex);
            }
            pgConnection=null;
        }

        if (jdbcConnection != null)
        {
            try {
                if (!jdbcConnection.isClosed()) {
                    jdbcConnection.close();
                }
            } catch (Exception ex) {
                log.error("  jdbcConnection.close()  error!",ex);
            }
            jdbcConnection=null;
        }

    }

    public PGReplicationStream getStream() {
        return stream;
    }

    public void setStream(PGReplicationStream stream) {
        this.stream = stream;
    }

    public PGConnection getPgConnection() {
        return pgConnection;
    }

    public void setPgConnection(PGConnection pgConnection) {
        this.pgConnection = pgConnection;
    }

    public Connection getJdbcConnection() {
        return jdbcConnection;
    }

    public void setJdbcConnection(Connection jdbcConnection) {
        this.jdbcConnection = jdbcConnection;
    }
}
