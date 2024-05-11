package com.boulderai.metabase.etl.tl.neo4j.service.handler;

import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;

import java.util.List;


/**
 * @ClassName: IDataHandler
 * @Description: 定义处理数据处理器接口
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public interface IDataHandler {

    /**
     * 具体处理过来的数据
     * @param dataList
     */
    void process(List<PgWalChangeAck> dataList,List<String>  cqlList);

    /**
     * 处理器名称
     * @return
     */
    String name();

    /**
     * 初始化函数
     */
    public void init();

    /**
     * 关闭时候函数
     */
    public void  close();


    /**
     * handler名称定义，Default
     */
    public static final String DEFAULT_DATA_HANDLER_NAME= "DefaultDataHandler";

    /**
     * handler名称定义，过滤器处理器名称
     */
    public static final String FILTER_DATA_HANDLER_NAME= "FilterDataHandler";

    /**
     * 最后工作处理器名称:做数据清理工作
     */
    public static final String LAST_DATA_HANDLER_NAME= "LastDataHandler";

    /**
     * 最后工作处理器名称:做数据清理工作
     */
    public static final String PERSIST_DATA_HANDLER_NAME= "PersistDataHandler";

    public static final String TRANSFORM_DATA_HANDLER_NAME= "TransformEventDataHandler";

    public static final String PG_PERSIST_DATA_HANDLER_NAME= "PgPersistDataHandler";

    public static final String RECORD_RELATION_DATA_HANDLER= "WalRecordRelationDataHandler";

    public static final String TRANSFORM_BEAN_DATA_HANDLER= "TransformBeanDataHandler";


    public static final String PERSIST_MODEL_DATA_HANDLER_NAME= "PersistModelDataHandler";
    public static final String RECORD_MODEL_DATA_RELATION_DATA_HANDLER= "WalRecordModelDataRelationHandler";


    public static final String FIELD_IS_DELETED ="is_deleted";



    public  int getPipelineId();
    public void setPipelineId(int pipelineId);

}
