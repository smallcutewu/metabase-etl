//package com.boulderai.metabase.sync.core.service.neo4j;
//
//import com.boulderai.metabase.sync.core.config.Neo4jConfig;
//import com.boulderai.metabase.sync.core.BaseTest;
//import com.boulderai.metabase.sync.core.service.wal.model.CqlValueContainer;
//import com.boulderai.metabase.sync.core.service.wal.model.EventType;
//import com.boulderai.metabase.sync.core.util.Neo4jPersistException;
//import org.junit.Assert;
//import org.junit.Test;
//import org.neo4j.driver.*;
//
//import java.util.ArrayList;
//import java.util.LinkedList;
//import java.util.List;
//
//import static org.neo4j.driver.Values.parameters;
//
//public class Neo4jDataRepositoryTest  extends BaseTest {
//
//    @Test
//    public void testInit()
//    {
//        Neo4jConfig config=  this.createNeo4jConfig();
//        Neo4jDataRepository  repository=new Neo4jDataRepository();
//        repository.setNeo4jConfig(config);
//        repository.init();
//        Driver driver =repository.getNeo4jDriver();
//        Assert.assertNotNull(driver);
//        repository.close();
//        driver =repository.getNeo4jDriver();
//        Assert.assertNull(driver);
//        repository.close();
//    }
//
//
//    @Test
//    public void testOperNeo4jDataByCql()
//    {
//        Neo4jConfig config=  this.createNeo4jConfig();
//        Neo4jDataRepository  repository=new Neo4jDataRepository();
//        repository.setNeo4jConfig(config);
//        repository.init();
//        Driver driver =repository.getNeo4jDriver();
//        Assert.assertNotNull(driver);
//        String cql="merge   ( t:my_person_test {id: $id} ) set t.name=$name ,t.age = $age  ";
//        String tableName="my_person_test";
//        CqlValueContainer cqlValueContainer=new CqlValueContainer( cql, EventType.UPDATE,  tableName);
//        LinkedList<Value> valueList=new LinkedList<>();
//        Object[] voArray=  new Object[]{"id",123,"name","zhangsan","age",20};
//        Value  value=parameters( voArray);
//        cqlValueContainer.setValueList(valueList);
//
//        Boolean res=repository.operNeo4jDataByCqlBatch(cqlValueContainer);
//        Assert.assertTrue(res);
//        repository.close();
//
//    }
//
//
//    @Test
//    public void testQueryByCql()
//    {
//        Neo4jConfig config=  this.createNeo4jConfig();
//        Neo4jDataRepository  repository=new Neo4jDataRepository();
//        repository.setNeo4jConfig(config);
//        repository.init();
//        Driver driver =repository.getNeo4jDriver();
//        Assert.assertNotNull(driver);
//        String cql=" match (n:Module)-[r]-(m:Module) return n,r,m  limit 1  ";
//        List<Record> list=repository.queryByCql(cql);
//        Assert.assertTrue(list.size()>0);
//        repository.close();
//    }
//
//
//    @Test
//    public void testQueryByCqlWithParameter()
//    {
//        Neo4jConfig config=  this.createNeo4jConfig();
//        Neo4jDataRepository  repository=new Neo4jDataRepository();
//        repository.setNeo4jConfig(config);
//        repository.init();
//        Driver driver =repository.getNeo4jDriver();
//        Assert.assertNotNull(driver);
//        String cql=" match (n:Module)-[r]-(m:Module) return n,r,m  limit $limit  ";
//        Object[] voArray=  new Object[]{"$limit",1};
//        Value  value=parameters( voArray);
//        Result res=repository.queryByCqlWithParameter( cql,   value);
//        Assert.assertNotNull(res);
//        repository.close();
//    }
//
//
//    @Test
//    public void testOperNeo4jDataByCqlBatch()
//    {
//        Neo4jConfig config=  this.createNeo4jConfig();
//        Neo4jDataRepository  repository=new Neo4jDataRepository();
//        repository.setNeo4jConfig(config);
//        repository.init();
//        Driver driver =repository.getNeo4jDriver();
//        Assert.assertNotNull(driver);
//        String cql=" match (n:Module)-[r]-(m:Module) return n,r,m  limit $limit  ";
//        Object[] voArray=  new Object[]{"$limit",1};
//        Value  value=parameters( voArray);
//        CqlValueContainer cqlValueContainer=new CqlValueContainer();
//        cqlValueContainer.setCql(cql);
//        LinkedList<Value> valueList=new LinkedList<Value>();
//        valueList.add(value);
//        cqlValueContainer.setValueList(valueList);
//        Boolean res=repository.operNeo4jDataByCqlBatch( cqlValueContainer);
//        Assert.assertTrue(res);
//        repository.close();
//    }
//
//
//    @Test
//    public void testOperNeo4jDataByCqlSingle()
//    {
//        Neo4jConfig config=  this.createNeo4jConfig();
//        Neo4jDataRepository  repository=new Neo4jDataRepository();
//        repository.setNeo4jConfig(config);
//        repository.init();
//        Driver driver =repository.getNeo4jDriver();
//        Assert.assertNotNull(driver);
//        String cql=" match (n:Module)-[r]-(m:Module) return n,r,m  limit $limit  ";
//        Object[] voArray=  new Object[]{"$limit",1};
//        Value  value=parameters( voArray);
//        CqlValueContainer cqlValueContainer=new CqlValueContainer();
//        cqlValueContainer.setCql(cql);
//        LinkedList<Value> valueList=new LinkedList<Value>();
//        valueList.add(value);
//        cqlValueContainer.setValueList(valueList);
//        Boolean res=repository.operNeo4jDataByCqlSingle( cqlValueContainer);
//        Assert.assertTrue(res);
//        repository.close();
//    }
//
//
//    @Test
//    public void testOperNeo4jDataByCqlBatchList()
//    {
//        Neo4jConfig config=  this.createNeo4jConfig();
//        Neo4jDataRepository  repository=new Neo4jDataRepository();
//        repository.setNeo4jConfig(config);
//        repository.init();
//        Driver driver =repository.getNeo4jDriver();
//        Assert.assertNotNull(driver);
//        String cql=" match (n:Module)-[r]-(m:Module) return n,r,m  limit 1 ";
//        List<String> cqlList=new ArrayList<String>();
//        cqlList.add(cql);
//        cql=" match (n:Module)-[r]-(m:Module) return n,r,m  limit 2 ";
//        cqlList.add(cql);
//        Boolean res=repository.operNeo4jDataByCqlBatch( cqlList);
//        Assert.assertTrue(res);
//        repository.close();
//    }
//
//
//    @Test
//    public void testOperNeo4jDataByCqlSingleError()
//    {
//        Neo4jConfig config=  this.createNeo4jConfig();
//        Neo4jDataRepository  repository=new Neo4jDataRepository();
//        repository.setNeo4jConfig(config);
//        repository.init();
//        Driver driver =repository.getNeo4jDriver();
//        Assert.assertNotNull(driver);
//        String cql="  (n:Module)-[r]-(m:Module) get n,r,m  limit  ";
//
//        Boolean res= repository.operNeo4jDataByCqlSingle( cql,true);
//        Assert.assertTrue(res);
//        repository.close();
//    }
//
//    private Driver  getDriver()
//    {
//        Neo4jConfig neo4jConfig=  this.createNeo4jConfig();
//        Driver    neo4jDriver = GraphDatabase.driver( neo4jConfig.getUri().trim(), AuthTokens.basic( neo4jConfig.getUsername().trim(),
//                neo4jConfig.getPassword().trim() ),neo4jConfig);
//        return  neo4jDriver;
//    }
//
//
//}
