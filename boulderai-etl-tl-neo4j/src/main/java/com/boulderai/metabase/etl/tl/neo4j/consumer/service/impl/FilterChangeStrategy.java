package com.boulderai.metabase.etl.tl.neo4j.consumer.service.impl;

import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.consumer.PgChangeEventConsumer;
import com.boulderai.metabase.etl.tl.neo4j.consumer.service.InterceptorStrategy;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.etl.tl.neo4j.util.DbOperateUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


@Service
public class FilterChangeStrategy implements InterceptorStrategy {

    private  final static Logger logger = LoggerFactory.getLogger(PgChangeEventConsumer.class);
    private static final Map<String,Map<String, Object>> tableMaps = new HashMap<>();

    private static final AtomicInteger filterCount = new AtomicInteger();

    @Autowired
    private PgWalConfig pgWalConfig;

    @Override
    public PgWalChange doInterceptor(PgWalChange change) {
        // 1.查出对应的那个字段
        // 2.change的before 和 after对比，找出哪些字段变更了
        if (change == null || !"update".equals(change.getKind()) || change.getBefore() == null) {
            return change;
        }
        Map<Object,Object> aftermap = change.getAfter();
        Map<Object,Object> beforemap = change.getBefore();
        Set changedKeys = getChangedAttribute(beforemap,aftermap);
        Map<String, Object> objectMap = tableMaps.get(change.getTable());
        String allowedAttribute = (String) objectMap.get("allowed_attribute");
        // 没配，变更事件放过去
        if (StringUtils.isEmpty(allowedAttribute)) {
            return change;
        }
        String [] allowedAttributes = allowedAttribute.split(",");
        boolean needFilter = true;
        for (int i= 0; i< allowedAttributes.length; i++) {
            if (changedKeys.contains(allowedAttributes[i])) {
                needFilter = false;
            }
        }
        // 3. 与1.中的字段来做比较，白名单策略，则通过，需要监控
        if (needFilter) {
            filterCount.incrementAndGet();
            if (filterCount.get()>=Integer.MAX_VALUE) {
                filterCount.set(0);
            }
            if (filterCount.get()%10000 == 0) {
                logger.info("已过滤日志量："+filterCount.get());
            }
            return null;
        } else {
            return change;
        }
    }

    @Override
    public String getStrategy() {
        return "filterChangeStrategy";
    }

    private Set getChangedAttribute(Map<Object,Object> before, Map<Object,Object> after) {
        Set<String> result = new HashSet();
        for(Object key: before.keySet()) {
            if (after.get(key) == null ) {
                if (before.get(key) == null) {
                    continue;
                } else {
                    result.add((String) key);
                    continue;
                }
            }
            if (!after.get(key).equals(before.get(key))) {
                result.add((String) key);
            }
        }
        return result;
    }


    public void refreshContext() {
        // 构建数据map
        filterCount.set(0);
        String[]  schemaList=pgWalConfig.getWalSubscribeSchema().split(",");
        List<Map<String, Object>> tableInfos = new ArrayList<>();
        for (int i= 0; i< schemaList.length; i++) {
            String schema  = schemaList[i];
            tableInfos.addAll(DbOperateUtil.queryMapList(DataSynConstant.PG_SUB_TABLES_SQL,schema));
        }
        for (Map<String, Object> tableInfo : tableInfos) {
            tableMaps.put((String) tableInfo.get("table_name"),tableInfo);
        }
    }

}
