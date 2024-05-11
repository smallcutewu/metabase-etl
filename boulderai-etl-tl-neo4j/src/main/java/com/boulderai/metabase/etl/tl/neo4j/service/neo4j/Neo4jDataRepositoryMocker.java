package com.boulderai.metabase.etl.tl.neo4j.service.neo4j;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.CqlValueContainer;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;

import java.util.ArrayList;
import java.util.List;

public class Neo4jDataRepositoryMocker implements INeo4jDataRepository {
    @Override
    public void init() {

    }

    @Override
    public List<Record> queryByCql(String cql) {
        return new ArrayList<Record>();
    }

    @Override
    public Result queryByCqlWithParameter(String cql, Value value) {
        return null;
    }

    @Override
    public Boolean operNeo4jDataByCqlBatch(CqlValueContainer cqlValueContainer) {
        return true;
    }

    @Override
    public Boolean operNeo4jDataByCqlSingle(CqlValueContainer cqlValueContainer) {
        return true;
    }

    @Override
    public Boolean operNeo4jDataByCqlBatch(List<String> cqlList) {
        return true;
    }

    @Override
    public Boolean operNeo4jDataByCqlSingleNoEx(String cql, Boolean debugLog) {
        return null;
    }



    @Override
    public Boolean operNeo4jDataByCqlSingle(String cql) {
        return true;
    }

    @Override
    public Boolean operNeo4jDataByCqlSingleNoExLog(String cql) {
        return null;
    }


    @Override
    public void executeBatchCql(List<String> cqlList) {

    }

    @Override
    public Boolean executeOneCql(List<String> cqlList) {
        return null;
    }

    @Override
    public Boolean executeSmallBatchCql(List<String> cqlList) {
        return null;
    }

    @Override
    public Boolean executeOneCqlWithSimple(String cql) {
        return null;
    }
}
