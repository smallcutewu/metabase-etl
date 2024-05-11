package com.boulderai.metabase.etl.tl.neo4j.service.pipeline;


import com.boulderai.metabase.etl.tl.neo4j.service.handler.IDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;

import java.util.List;


/**
 * @ClassName: IProcessPipeline
 * @Description: 定义处理Pipeline接口
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public interface IProcessPipeline {

    /**
     * 将handler加入队列最前面
     * @param handleName
     * @param dataHandler
     * @return 返回pipeline本身，链式编程
     */
    IProcessPipeline addFirst(String handleName, IDataHandler dataHandler);

    /**
     * 将handler加入队列最后面
     * @param handleName
     * @param dataHandler
     * @return 返回pipeline本身，链式编程
     */
    IProcessPipeline addLast(String handleName, IDataHandler dataHandler);

    /**
     * 启动pipeline
     */
    public void start();

    /**
     * 处理wal数据
     * @param dataList  wal数据
     */
    public void   handleWalData(List<PgWalChangeAck> dataList);

    /**
     *  增加wal记录进入消息队列
     * @param record wal记录
     */
    public Boolean addWalRecord(PgWalChangeAck record);

    public void   addHandlers();

    public Boolean  isQueueFull();

}
