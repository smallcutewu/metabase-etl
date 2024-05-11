package com.boulderai.metabase.etl.tl.neo4j.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.PostgresConnectionManager;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.*;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.SQLException;
import java.util.List;
import java.util.Map;
/**
 * @ClassName: DbOperateUtil
 * @Description: 操作数据库常用函数定义
 * @author  df.l
 * @date 2022年09月08日
 * @Copyright boulderaitech.com
 */
public class DbOperateUtil {
    private  final static Logger logger = LoggerFactory.getLogger(DbOperateUtil.class);

    /**
     * 根据sql和参数列表查询表内容信息
     * @param sql
     * @param parameters
     * @return  查询结果
     */
    public static List<Map<String, Object>> queryMapList( String sql, Object ... parameters)
    {
        QueryRunner queryRunner = createQueryRunner();
        List<Map<String, Object>>  list= null;
        try {
            list = queryRunner.query(sql,new MapListHandler(),parameters);
        } catch (SQLException ex) {
            logger.error("query error! sql: "+sql,parameters,ex);
        }
        return list;
    }


    public static boolean queryTest(String sql, Object ... parameters)
    {
        QueryRunner queryRunner = createQueryRunner();
        try {
            queryRunner.query(sql,new MapHandler(),parameters);
            return true;
        } catch (SQLException ex) {
            logger.error("query error! sql: "+sql,parameters,ex);
        }
        return false;
    }


    public static  Map<String, Object> queryMap( String sql, Object ... parameters)
    {
        QueryRunner queryRunner = createQueryRunner();
        Map<String, Object>  map= null;
        try {
            map = queryRunner.query(sql,new MapHandler(),parameters);
        } catch (SQLException ex) {
            logger.error("query error! sql: "+sql,parameters,ex);
        }
        return map;
    }

    public static  <T> List<T> queryBeanList( String sql,Class<T> type, Object ... parameters)
    {
        QueryRunner queryRunner = createQueryRunner();
        List<T>  list= null;
        try {
            list = queryRunner.query(sql,new BeanListHandler<T>(type),parameters);
        } catch (SQLException ex) {
            logger.error("queryBeanList error! sql: "+sql,parameters,ex);
        }
        return list;
    }

    /**
     * 创建执行QueryRunner
     * @return jdbc执行器
     */
    private static QueryRunner  createQueryRunner()
    {
        PostgresConnectionManager connectionManager =    SpringApplicationContext.getBean(PostgresConnectionManager.class);
        DruidDataSource dataSource = connectionManager.getDruidDataSource();
        QueryRunner queryRunner = new QueryRunner(dataSource);
        return queryRunner;
    }

    /**
     * 批量更新数据
     * @param sql  sql
     * @param parameters  修改参数列表
     */
    public static int[]  batchUpdate( String sql, List<Object[]>  parameters)
    {
        QueryRunner queryRunner = createQueryRunner();
        try {
           return   queryRunner.batch(sql, parameters.toArray(new Object[parameters.size()][]));
        } catch (SQLException ex) {
            logger.error("batchUpdate error, will try update one by one ! sql: "+sql,parameters,ex);
            for (Object[] tempParas: parameters) {
                update( sql, tempParas);
            }
        }
        return null;
    }

    public static int[]  batchUpdate( String sql, Object[][]  parameters)
    {
        QueryRunner queryRunner = createQueryRunner();
        try {
            return   queryRunner.batch(sql,parameters);
        } catch (SQLException ex) {
            logger.error("batchUpdate error, will try update one by one ! sql: "+sql,parameters,ex);
            for (Object[] tempParas: parameters) {
                update( sql, tempParas);
            }
        }
        return null;
    }

    /**
     * 更新记录
     * @param sql  sql
     * @param parameters   更新参数
     */
    public static int   update(String sql,Object[]  parameters)
    {
        QueryRunner queryRunner = createQueryRunner();
        try {
          return   queryRunner.update(sql, parameters);
        } catch (SQLException ex) {
            logger.error("update error! sql: "+sql,parameters,ex);
        }
        return  0;
    }

    public static int   update(String sql)
    {
        QueryRunner queryRunner = createQueryRunner();
        try {
            return   queryRunner.update(sql);
        } catch (SQLException ex) {
            logger.error("update error! sql: "+sql,ex);
        }
        return  0;
    }
    public static Long   insert(String sql,Object[]  parameters)
    {
        QueryRunner queryRunner = createQueryRunner();
        try {
            return  queryRunner.insert(sql, new ScalarHandler<Long>(), parameters);
        } catch (SQLException ex) {
            logger.error("insert error! sql: "+sql,parameters,ex);
        }
        return  0L;
    }

    public  static Boolean  batchInsert(String sql ,Object[][] params)
    {
        QueryRunner queryRunner = createQueryRunner();
        int[] result=null;
        try
        {
            result=  queryRunner.batch(sql, params);
        }
        catch(Exception ex)
        {
            logger.error("sql批量更新删除失败! "+sql,ex);
            return false;
        }
        return true;
    }




    /**
     * 得到查询记录的条数
     *
     * @param sql
     * @param params
     * @return
     */
    public static int getCount(String sql, Object... params) {
        QueryRunner queryRunner = createQueryRunner();
        try {
            // ScalarHandler 将ResultSet的一个列到一个对象
            Object value = queryRunner.query( sql, new ScalarHandler<>(), params);
            return NumberUtils.toInt(String.valueOf(value),0);
        } catch (Exception ex) {
            logger.error("getCount error! sql: {}",sql,ex);
//            return   -100
        }
        return 0;
    }
}
