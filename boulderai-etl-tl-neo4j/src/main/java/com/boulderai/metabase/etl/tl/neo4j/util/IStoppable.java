package com.boulderai.metabase.etl.tl.neo4j.util;

/**
 * @ClassName: Stopper
 * @Description: wal消费者停止标志
 * @author  df.l
 * @date 2022年8月11日
 * @Copyright boulderaitech.com
 */
public interface IStoppable {
    /**
     * 应用停止处理接口定义
     * @param cause 停止原因
     */
    public  void stop(String cause);
}
