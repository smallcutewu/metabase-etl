package com.boulderai.metabase.etl.tl.neo4j.service.pipeline;

import com.boulderai.metabase.etl.tl.neo4j.service.handler.DefaultDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.FilterDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.config.Neo4jConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.IDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class DefaultMqProcessPipeline implements  IProcessPipeline{

    protected   final static Logger logger = LoggerFactory.getLogger(DefaultMqProcessPipeline.class);

    /**
     * 存贮默认数据处理器，先进先出处理
     */
    private final ConcurrentLinkedDeque<IDataHandler> dataHandlerQueue = new ConcurrentLinkedDeque<IDataHandler>();

    private static  final int QUEUE_BUFFER_MAX_SIZE=60000;

    /**
     * 存放wal消息队列
     */
    protected final LinkedBlockingQueue<PgWalChangeAck> walRecordChangeQueue = new LinkedBlockingQueue<PgWalChangeAck>(QUEUE_BUFFER_MAX_SIZE);

    private  int pipId;
    private Neo4jConfig neo4jConfig;


    protected final List<PgWalChangeAck> changeList=new ArrayList<PgWalChangeAck>();
    public void addChange(PgWalChangeAck  change)
    {
        changeList.clear();
        changeList.add(change);
    }

    public void addChanges(List<PgWalChangeAck>  changes)
    {
        changeList.clear();
        changeList.addAll(changes);
    }

    public void handleDataList()
    {
        this.handleWalData(changeList);
    }

    public void clearChange()
    {
        if(!changeList.isEmpty())
        {
            for (PgWalChangeAck change : changeList) {
                change.clear();
            }
            changeList.clear();
        }

    }

    public void setNeo4jConfig(Neo4jConfig neo4jConfig) {
        this.neo4jConfig = neo4jConfig;
    }

    public DefaultMqProcessPipeline(int pipId)
    {
        this.pipId=pipId;
    }


    @Override
    public void start() {
    }

    @Override
    public void handleWalData(List<PgWalChangeAck> dataList) {
        List<String>  cqlList=new ArrayList<>();
        dataHandlerQueue.stream().forEach(handler ->
        {
            if (CollectionUtils.isEmpty(dataList)) {
                return;
            }
            handler.process(dataList,cqlList);
        });
        cqlList.clear();

    }

    /**
     * 将handler加入队列最前面
     *
     * @param handleName
     * @param dataHandler
     * @return 返回pipeline本身，链式编程
     */
    @Override
    public IProcessPipeline addFirst(String handleName, IDataHandler dataHandler) {
        dataHandler.setPipelineId(pipId);
        dataHandlerQueue.addFirst(dataHandler);
        return this;
    }

    public IProcessPipeline addFirst(IDataHandler dataHandler) {
        dataHandler.setPipelineId(pipId);
        dataHandlerQueue.addFirst(dataHandler);
        return this;
    }


    /**
     * 将handler加入队列最后面
     *
     * @param handleName
     * @param dataHandler
     * @return 返回pipeline本身，链式编程
     */
    @Override
    public IProcessPipeline addLast(String handleName, IDataHandler dataHandler) {
        dataHandler.setPipelineId(pipId);
        dataHandlerQueue.addLast(dataHandler);
        return this;
    }

    public IProcessPipeline addLast(IDataHandler dataHandler) {
        dataHandler.setPipelineId(pipId);
        dataHandlerQueue.addLast(dataHandler);
        return this;
    }

    @Override
    public Boolean addWalRecord(PgWalChangeAck record) {
        return null;
    }

    @Override
    public  void addHandlers()
    {
        this.addFirst(new DefaultDataHandler());
        this.addLast(new FilterDataHandler());
    }

    @Override
    public Boolean isQueueFull() {
        return false;
    }


}
