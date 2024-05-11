package com.boulderai.metabase.etl.tl.neo4j.service.neo4j;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.CqlValueContainer;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;

import java.util.List;

public interface INeo4jDataRepository {
    public void init();

    public List<Record> queryByCql(String cql);

    public Result queryByCqlWithParameter(String cql, Value value);

    public Boolean operNeo4jDataByCqlBatch(CqlValueContainer cqlValueContainer);

    public Boolean operNeo4jDataByCqlSingle(CqlValueContainer cqlValueContainer);

    public Boolean operNeo4jDataByCqlBatch(List<String> cqlList);

    public Boolean operNeo4jDataByCqlSingleNoEx(String cql, Boolean debugLog);

    public Boolean operNeo4jDataByCqlSingle(String cql);

    //    public void setNeo4jConfig(Neo4jConfig neo4jConfig);
    public Boolean operNeo4jDataByCqlSingleNoExLog(String cql);

    public void executeBatchCql(List<String> cqlList);

    public Boolean executeOneCql(List<String> cqlList);

    public Boolean executeSmallBatchCql(List<String> cqlList);

    public Boolean executeOneCqlWithSimple(String cql);

}
