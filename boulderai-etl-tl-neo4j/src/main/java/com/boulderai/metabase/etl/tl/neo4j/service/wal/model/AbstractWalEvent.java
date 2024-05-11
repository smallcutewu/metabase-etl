package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import org.postgresql.replication.LogSequenceNumber;


/**
 * @ClassName: AbstractWalEvent
 * @Description: pg wal logseq基础组装模型
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */

public class AbstractWalEvent {
    private LogSequenceNumber logSequenceNumber;
    private LogSequenceNumber nextLogSequenceNumber;

    public LogSequenceNumber getNextLogSequenceNumber() {
        return nextLogSequenceNumber;
    }

    public void setNextLogSequenceNumber(LogSequenceNumber nextLogSequenceNumber) {
        this.nextLogSequenceNumber = nextLogSequenceNumber;
    }

    public LogSequenceNumber getLogSequenceNumber() {
        return logSequenceNumber;
    }

    public void setLogSequenceNumber(LogSequenceNumber logSequenceNumber) {
        this.logSequenceNumber = logSequenceNumber;
    }

    public void clear()
    {

    }
}
