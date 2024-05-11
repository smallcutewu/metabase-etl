package com.boulderai.metabase.etl.tl.neo4j.util;

/**
 * @ClassName: AppCurrentState
 * @Description: 应用状态定义
 * @author  df.l
 * @date 2022年09月05日
 * @Copyright boulderaitech.com
 */
public enum AppCurrentState {

    /**
     * active服务器
     */
    MASTER(1,"master server"),

    /*8
     * waitting 服务器
     */
    SLAVE (0,"slave server");

    /**
     * 状态
     */
    private int status;

    /**
     * 状态描述
     */
    private String desc;

    private AppCurrentState(int status,String desc)
    {
        this.status=status;
        this.desc=desc;
    }

    public int getStatus() {
        return status;
    }
}
