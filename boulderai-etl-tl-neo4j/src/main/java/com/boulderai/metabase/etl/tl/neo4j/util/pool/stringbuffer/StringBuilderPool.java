//package com.boulderai.metabase.sync.core.util.pool.stringbuffer;
//
//import com.boulderai.metabase.sync.core.config.StringPoolConfig;
//import org.apache.commons.pool2.impl.GenericObjectPool;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * @ClassName: StringBuilderPool
// * @Description: StringBuilder实例池，在wal同步中大量使用
// * @author  df.l
// * @date 2022年09月05日
// * @Copyright boulderaitech.com
// */
//public class StringBuilderPool {
//    private   final static Logger logger = LoggerFactory.getLogger(StringBuilderPool.class);
//    private final GenericObjectPool<StringBuilder> innerPool;
//
//    private StringBuilderPool( StringPoolConfig config) {
//        PooledStringBuilderFactory stringBuilderFactory=new PooledStringBuilderFactory();
//        innerPool = new GenericObjectPool<StringBuilder>(stringBuilderFactory);
//        innerPool.setMaxTotal(config.getMaxTotal());
//        innerPool.setMinIdle(config.getMinIdle());
//        innerPool.setMaxIdle(config.getMaxIdle());
//        innerPool.setMinEvictableIdleTimeMillis(config.getMinEvictableIdleTimeMillis());
//        innerPool.setSoftMinEvictableIdleTimeMillis(config.getSoftMinEvictableIdleTimeMillis());
//        innerPool.setTimeBetweenEvictionRunsMillis(config.getTimeBetweenEvictionRunsMillis());
//        innerPool.setTestOnBorrow(config.getTestOnBorrow());
//    }
//
//
//    public StringBuilder borrowObject()  {
//        StringBuilder  builder= null;
//        try {
//            builder = innerPool.borrowObject();
//        } catch (Exception e) {
//            logger.error("borrow object from pool error!",e);
//        }
//        if(builder!=null)
//        {
//            builder.setLength(0);
//        }
//        else
//        {
//            builder= new StringBuilder();
//        }
//        return builder;
//    }
//
//    public StringBuilder borrowObject(long borrowMaxWaitMillis) throws Exception {
//        return innerPool.borrowObject(borrowMaxWaitMillis);
//    }
//
//    public void returnObject(StringBuilder builder) {
//        if(builder!=null)
//        {
//            builder.setLength(0);
//            innerPool.returnObject(builder);
//        }
//    }
//
//    private static class Holder
//    {
//        //暂时不用配置在nacos
//        private final static   StringBuilderPool  INSTANCE=new StringBuilderPool(new StringPoolConfig());
//    }
//
//    public static StringBuilderPool  getInstance() {
//        return Holder.INSTANCE;
//    }
//
//}
