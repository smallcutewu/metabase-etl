package com.boulderai.metabase.etl.tl.neo4j.service.neo4j;

import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.boulderai.metabase.etl.tl.neo4j.util.SpringApplicationContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @ClassName: Neo4jDataRepositoryContext
 * @Description: neo4j上下文
 * @author  df.l
 * @date 2022年10月16日
 * @Copyright boulderaitech.com
 */
public class Neo4jDataRepositoryContext {
    private static final Map<Integer,INeo4jDataRepository> REPOSITORY_MAP = new ConcurrentHashMap<Integer,INeo4jDataRepository>() ;
    private static INeo4jDataRepository  defaultNeo4jDataRepository;
    private static ReentrantLock  lock=new ReentrantLock();

    public static  void set(INeo4jDataRepository  res,int pipeId)
    {
//        REPOSITORY_MAP.put(pipeId,res);
    }

    public static  INeo4jDataRepository get(int pipeId)
    {
//        return REPOSITORY_MAP.get(pipeId);
        if(defaultNeo4jDataRepository==null)
        {
            defaultNeo4jDataRepository=   SpringApplicationContext.getBean(Neo4jDataRepository.class);
        }
        return  defaultNeo4jDataRepository;
    }

    public static  void remove(int pipeId)
    {
        REPOSITORY_MAP.remove(pipeId);
    }

    public static INeo4jDataRepository getDefaultNeo4jDataRepository() {
        return defaultNeo4jDataRepository;
    }

    public static void init(Neo4jConfig neo4jConfig, int pipeId)
    {
        if(defaultNeo4jDataRepository==null)
        {
            defaultNeo4jDataRepository=   SpringApplicationContext.getBean(Neo4jDataRepository.class);
        }
//        if(!REPOSITORY_MAP.containsKey(pipeId))
//        {
//            Neo4jDataRepository res=   SpringApplicationContext.getBean(Neo4jDataRepository.class);
////            res.setNeo4jConfig(neo4jConfig);
//            res.init();
//            set(res,pipeId);
//        }
    }
}
