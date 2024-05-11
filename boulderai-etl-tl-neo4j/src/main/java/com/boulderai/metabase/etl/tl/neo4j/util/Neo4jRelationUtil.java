package com.boulderai.metabase.etl.tl.neo4j.util;

import com.google.gson.Gson;
import org.apache.commons.collections4.CollectionUtils;
import org.neo4j.driver.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @ClassName: Neo4jRelationUtil
 * @Description: neo4j关系解析器
 * @author  df.l
 * @date 2022年10月26日
 * @Copyright boulderaitech.com
 */
public class Neo4jRelationUtil {
    private  final static Gson gson = new Gson();
    private  final static Logger logger = LoggerFactory.getLogger(Neo4jRelationUtil.class);

    public static Map<String, Set<Map<String ,Object>>> parseRelations(List<Record> resultList)
    {
        Map<String, Set<Map<String ,Object>>> retMap =null;

        if(CollectionUtils.isNotEmpty(resultList))
        {
            retMap = new HashMap<String, Set<Map<String ,Object>>>();
            Set<Map<String ,Object>> leftNodes = new HashSet<>();
            Set<Map<String ,Object>> edges = new HashSet<>();
            Set<Map<String ,Object>> rightNodes = new HashSet<>();
            parseNodeRelationList(resultList, leftNodes,edges,rightNodes);
            retMap.put("leftNodes",leftNodes);
            retMap.put("edges",edges);
            retMap.put("rightNodes",rightNodes);

    //        String mapStr=gson.toJson(retMap);
    //        System.out.println(mapStr);
        }
        return retMap;
    }
    /**
     * cql的return返回多种节点match (n)-[edge]-(n) return n,m,edge：限定返回关系时，关系的别名必须“包含”edge
     * @param lists 和cql的return返回节点顺序对应
     * @return List<Map<String,Object>>
     */
    private static <T> void parseNodeRelationList(List<Record> resultList, Set<T>... lists) {
        for (Record r : resultList) {
            if (r.size() != lists.length) {
                logger.error("节点数和lists数不匹配");
                return;
            }
        }
        //用于给每个Set list赋值
        int listIndex = 0;

        for (Record r : resultList) {
            for (String index : r.keys()) {
                //对于关系的封装
                if (index.contains("edge")) {
                    Map<String, Object> map = new HashMap<>();
                    //关系上设置的属性
                    map.putAll(r.get(index).asMap());
                    //外加三个固定属性
                    map.put("edgeId", r.get(index).asRelationship().id());
                    map.put("edgeFrom", r.get(index).asRelationship().startNodeId());
                    map.put("edgeTo", r.get(index).asRelationship().endNodeId());
                    lists[listIndex++].add((T) map);
                }
                //对于节点的封装
                else {
                    Map<String, Object> map = new HashMap<>();
                    //节点上设置的属性
                    map.putAll(r.get(index).asMap());
                    //外加一个固定属性
                    map.put("nodeId", r.get(index).asNode().id());
                    lists[listIndex++].add((T) map);
                }
            }
            listIndex = 0;
        }

    }
}
