package com.boulderai.metabase.etl.tl.neo4j.zookeeper;

import com.boulderai.metabase.etl.tl.neo4j.util.AppCurrentState;

/**
 * @ClassName: AppStateListener
 * @Description: 应用状态变换监听器
 * @author  df.l
 * @date 2022年09月05日
 * @Copyright boulderaitech.com
 */
public interface AppStateListener {

    /**
     * 当应用状态发生变换时候需要处理事情定义
     * @param state  状态： master slaver
     */
    void onChange(AppCurrentState state);
}
