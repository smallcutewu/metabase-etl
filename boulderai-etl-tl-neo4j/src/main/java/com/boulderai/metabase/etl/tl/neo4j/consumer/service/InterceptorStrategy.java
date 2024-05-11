package com.boulderai.metabase.etl.tl.neo4j.consumer.service;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;

/**
 *
 */
public interface InterceptorStrategy {

    /**
     *
     * @param change
     * @return
     */
    public PgWalChange doInterceptor(PgWalChange change);

    /**
     *
     * @return
     */
    String getStrategy();

    /**
     *
     */
    void refreshContext();

}
