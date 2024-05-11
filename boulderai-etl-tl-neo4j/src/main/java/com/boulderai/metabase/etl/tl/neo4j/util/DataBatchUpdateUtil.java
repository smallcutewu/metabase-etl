package com.boulderai.metabase.etl.tl.neo4j.util;

import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.INeo4jDataRepository;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepositoryContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.Neo4jSyncContext;
import com.boulderai.metabase.etl.tl.neo4j.util.exception.SqlQueryException;
import com.boulderai.metabase.lang.util.SleepUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class DataBatchUpdateUtil {
//    private static final int PAGE_SIZE = 10000;
//    private static final int BATCH_SIZE = 5000;
//    private static final int MAX_SIZE = 30000;

    //    private static final int PAGE_SIZE = 10000;
    private static final int BATCH_SIZE = 1000;
    private static final int MAX_SIZE = 3000;


    private static final String SELECT_ID_SQL = "SELECT %s FROM %s.%s ORDER BY %s LIMIT ? OFFSET ? ";
    private static final String UPDATE_QUERY_SQL = " UPDATE %s.%s  SET updated_at = COALESCE(updated_at, current_timestamp) + INTERVAL '2 second'   WHERE %s = ?";
    private static final String TABLE_COUNT_SQL = " SELECT COUNT(1) FROM %s.%s ";
    private static final String ALL_UPDATE_QUERY_SQL = " UPDATE %s.%s  SET updated_at = COALESCE(updated_at, current_timestamp) + INTERVAL '2 second'   ";
    private static final String SELECT_TABLE_PKEY_SQL = " SELECT CONSTRAINT_COLUMN_USAGE.column_name   as pkey  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS   " +
            "            LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE          ON TABLE_CONSTRAINTS.constraint_name = CONSTRAINT_COLUMN_USAGE.constraint_name   " +
            "            WHERE TABLE_CONSTRAINTS.table_name =  '%s'  AND  CONSTRAINT_COLUMN_USAGE.table_name = '%s'  " +
            "            and TABLE_CONSTRAINTS.constraint_schema='%s'  and TABLE_CONSTRAINTS.CONSTRAINT_TYPE ='PRIMARY KEY'  limit 1  ";

    private static final String SELECT_TABLE_PKEY_DATA_TYPE_SQL = "SELECT c.column_name, c.data_type,c.table_name " +
            "  FROM information_schema.columns c " +
            "  WHERE c.table_schema = '%s' AND c.table_name = '%s' AND c.column_name ='%s';";
    private static Map<String, String> pkMap = new ConcurrentHashMap<>();


    private final static Neo4jSyncContext neo4jSyncContext = Neo4jSyncContext.getInstance();

    private final static ConcurrentHashMap<String, Boolean> IS_TABLE_PKEY_DATA_TYPE_STRING_MAP = new ConcurrentHashMap<>();


    public static void clearPkMap() {
        pkMap.clear();
        IS_TABLE_PKEY_DATA_TYPE_STRING_MAP.clear();
    }

    public static int updateTableDeta(String schema, String tableName) {
//        String pkeySql=String.format(SELECT_TABLE_PKEY_SQL, tableName,tableName,schema);
//        String pkey=pkMap.get(tableName);
        String allSql = String.format(ALL_UPDATE_QUERY_SQL, schema, tableName);
        allSql += allSql + "  where updated_at  >= '" + DateTime.now().toString(DataSynConstant.DATE_SHORT_FORMAT) + " 08:30:00' ";
        DbOperateUtil.update(allSql);
        return 1;
    }

    /**
     * 获取schma下的table的主键信息，这里是查询过的数据直接放在缓存中，定时任务每次清空缓存
     *
     * @param schema
     * @param tableName
     * @return
     */
    private static String getPrimaryKey(String schema, String tableName) {
        String pkey = pkMap.get(tableName);
        if (MapUtils.isEmpty(pkMap) || !pkMap.containsKey(tableName)) {
            //获取某个schema下table的主键
            String pkeySql = String.format(SELECT_TABLE_PKEY_SQL, tableName, tableName, schema);
            Map<String, Object> pkeyMap = DbOperateUtil.queryMap(pkeySql);
            if (!MapUtils.isEmpty(pkeyMap)) {
                pkey = (String) pkeyMap.get("pkey");
                if (StringUtils.isNotBlank(pkey)) {
                    pkMap.put(tableName, pkey);
                }
            }
            pkey = pkMap.get(tableName);
        }
        return pkey;
    }

    //todo: 全表级别的数据批量更新
    private static boolean updateInBatches() {
        return false;
    }

    //暂时的策略，全表级别更新数据
    private static int updateAllData(String allSql) {
        DbOperateUtil.update(allSql);
        log.info("DataBatchUpdateUtil redo sql for sync : " + allSql);
        SleepUtil.sleepSecond(10);
        return 1;
    }

    /**
     * 检查主键的数据类型是否是文本
     *
     * @param schema
     * @param tableName
     * @param pkey
     * @return
     */
    public static boolean isColumnTypeString(String schema, String tableName, String pkey) {
        String pkeyDataTypeSql = String.format(SELECT_TABLE_PKEY_DATA_TYPE_SQL, schema, tableName, pkey);
        Map<String, Object> pkeyDataTypeMap = DbOperateUtil.queryMap(pkeyDataTypeSql);
        if (MapUtils.isEmpty(pkeyDataTypeMap)) {
            throw new SqlQueryException(pkeyDataTypeSql);
        }

        String dataType = (String) pkeyDataTypeMap.get("data_type");
        if (StringUtils.isEmpty(dataType)) {
            throw new SqlQueryException(pkeyDataTypeSql);
        }

        return dataType.contains("char") || dataType.contains("character") || dataType.contains("text") || dataType.contains("hstore");
    }

    /**
     * 懒加载缓存
     *
     * @param schema
     * @param tableName
     * @param pkey
     * @return
     */
    public static boolean getPkeyDataType(String schema, String tableName, String pkey) {
        return IS_TABLE_PKEY_DATA_TYPE_STRING_MAP.computeIfAbsent(tableName, key -> isColumnTypeString(schema, tableName, pkey));
    }


    /**
     * CQL: 主键IN语句查询
     *
     * @param idList
     * @param schema
     * @param tableName
     * @param pkey
     * @return
     */
    public static String queryIdsCql(List<Object> idList, String schema, String tableName, String pkey) {
        StringBuilder sb = new StringBuilder();
        sb.append("MATCH (t:").append(tableName)
                .append(") WHERE t.").append(pkey).append(" IN [")
                .append(idList.stream()
                        .map(id -> {
                            if (getPkeyDataType(schema, tableName, pkey)) {
                                return "'" + id + "'";
                            } else {
                                return String.valueOf(id);
                            }
                        })
                        .collect(Collectors.joining(",")))
                .append("] return t.").append(pkey).append(" as ").append(pkey);

        return sb.toString();
    }

    /**
     * 查询主键在图库中
     *
     * @param idList
     * @param schema
     * @param tableName
     * @param pkey
     * @return
     */
    public static Set<Object> queryIdsInNeo4j(List<Object> idList, String schema, String tableName, String pkey) {
        INeo4jDataRepository neo4jDataRepository = Neo4jDataRepositoryContext.get(0);

        List<Record> records = neo4jDataRepository.queryByCql(queryIdsCql(idList, schema, tableName, pkey));

        return records.stream().map(node -> {
            Value value = node.get(pkey);
            return value.isNull() ? null : value.asObject();
        }).filter(Objects::nonNull).collect(Collectors.toSet());
    }

    /**
     * 比较并同步
     *
     * @param schema
     * @param tableName
     * @param pkey
     * @param totalRecords
     * @return
     */
    public static boolean compareAndSync(String schema, String tableName, String pkey, int totalRecords) {
        String updateSql = String.format(UPDATE_QUERY_SQL, schema, tableName, pkey);
        log.info("DataBatchUpdateUtil redo sql for sync : " + updateSql);
        int totalPages = (int) Math.ceil((double) totalRecords / BATCH_SIZE);
        boolean updateSucess = true;

        for (int currentPage = 0; currentPage < totalPages; currentPage++) {
            //获取当前页数据
            List<Object> idList = getIdListFromDatabase(currentPage, schema, tableName, pkey);
            if (!CollectionUtils.isEmpty(idList)) {
                //查询图库的数据
                Set<Object> idSet = queryIdsInNeo4j(idList, schema, tableName, pkey);

                List<Object> notInSet = idList.stream()
                        .filter(obj -> !(idSet.contains(obj) || idSet.contains(obj.toString())))
                        .collect(Collectors.toList());

                //拼接In语句SQL更新
                boolean updateResult = updatePgToSyncNeo4j(notInSet, updateSql);
                if (!updateResult) {
                    log.error("更新PG数据失败：{}", updateSql);
                    updateSucess = false;
                }
            }
        }

        return updateSucess;
    }

    /**
     * 同步PG数据，刷新数据更新时间
     *
     * @param idList
     * @param updateSql
     * @return
     */
    private static boolean updatePgToSyncNeo4j(List<Object> idList, String updateSql) {
        if (CollectionUtils.isEmpty(idList)) {
            return true;
        }

        Object[] idListParameters = new Object[idList.size()];
        idList.toArray(idListParameters);
        Object[][] params = new Object[idList.size()][1];
        for (int i = 0; i < idList.size(); i++) {
            params[i][0] = idListParameters[i];
        }
        log.info("sql:{},params:{}", updateSql, params);
        int[] updateIds = DbOperateUtil.batchUpdate(updateSql, params);
        return updateIds != null && updateIds.length >= idList.size();
    }

    public static int updateTablePaging(String schema, String tableName) {
        String pkey = getPrimaryKey(schema, tableName);
        String allSql = String.format(ALL_UPDATE_QUERY_SQL, schema, tableName);

        //没有主键的，直接全量更新，这里最好分批
        if (StringUtils.isEmpty(pkey)) {
            return updateAllData(allSql);
        }

        String tableCountSql = String.format(TABLE_COUNT_SQL, schema, tableName);
        int totalRecords = DbOperateUtil.getCount(tableCountSql);
        if (totalRecords <= 0) {
            return 0;
        }

        if (totalRecords < MAX_SIZE) {
            return updateAllData(allSql);
        } else {
            boolean updateSuccess = compareAndSync(schema, tableName, pkey, totalRecords);
            log.info("job sleep 20 sec! " + tableName);
            SleepUtil.sleepSecond(20);
            return updateSuccess ? 1 : -1;
        }

    }


    private static List<Object> getIdListFromDatabase(int currentPage, String schema, String tableName, String pkey) {
        List<Object> idList = new ArrayList<>();
        String sql = String.format(SELECT_ID_SQL, pkey, schema, tableName, pkey);
        int offset = currentPage * BATCH_SIZE;
        List<Map<String, Object>> idMapList = DbOperateUtil.queryMapList(sql, BATCH_SIZE, offset);
        if (CollectionUtils.isNotEmpty(idMapList)) {
            for (Map<String, Object> map : idMapList) {
                Object id = map.get(pkey);
                if (id != null) {
                    idList.add(id);
                }
            }
            idMapList.clear();
        }
        return idList;
    }
}

