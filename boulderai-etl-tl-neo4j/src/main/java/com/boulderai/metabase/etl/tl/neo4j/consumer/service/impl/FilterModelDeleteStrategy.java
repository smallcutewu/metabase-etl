package com.boulderai.metabase.etl.tl.neo4j.consumer.service.impl;

import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.consumer.service.InterceptorStrategy;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.EventType;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
public class FilterModelDeleteStrategy implements InterceptorStrategy {

    @Autowired
    private PgWalConfig pgWalConfig;

    /**
     *
     * @param change
     * @return
     */
    public PgWalChange doInterceptor(PgWalChange change) {
        if (!pgWalConfig.getDeleteModelfilter() || change == null) {
            return change;
        }
        if (EventType.DELETE.toString().equals(change.getKind())) {
            if (DataSynConstant.METATABLELIST.contains(change.getTable())) {
                return change;
            }
                return null;
        }
        return change;
    }

    /**
     *
     * @return
     */
    public String getStrategy() {
        return "filterModelDeleteStrategy";
    }

    /**
     *
     */
    public void refreshContext() {

    }
}
