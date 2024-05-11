package com.boulderai.metabase.etl.tl.neo4j.service.handler;

import com.boulderai.metabase.lang.exception.MetabaseException;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.INeo4jDataRepository;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepositoryContext;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.Column;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChangeAck;
import com.boulderai.metabase.etl.tl.neo4j.util.ExceptionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: BaseDataHandler
 * @Description: 定义数据处理模块，通过模板方法让子类来继承实现
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
@Slf4j
public abstract class BaseDataHandler   implements IDataHandler {
    protected boolean  relogError = true;

    private int pipelineId=-1;

    protected static final String DA_LOGIC_ENTITY_TABLE="da_logic_entity";
    protected static final String DA_ENTITY_ATTRIBUTE_TABLE="da_entity_attribute";
    protected static final String DA_LOGIC_RELATION_TABLE="da_logic_relation";
    protected static final String  ENTITY_ID_COLUMN = "entity_id";
    protected static final String  LOG_ENTITY_ID_COLUMN = "id";
    protected static final String  TABLE_NAME_COLUMN = "table_name";
    protected static final String  START_ID_COLUMN = "start_id";
    protected static final String  END_ID_COLUMN = "end_id";
    protected static final String FIELD_TYPE_STRING="String";
    protected static final String FIELD_TYPE_BOOLEAN="Boolean";
    protected static final String FIELD_TYPE_FLOAT="Float";
    protected static final String DELETE_MODULE_BY_HANDLE="match (n:Module{module_id:'${entityId}'}) detach delete n ";
    protected static final String DELETE_FIELD_BY_HANDLE ="match (n:Field{module_id:'${entityId}'}) detach delete n ";
    protected  Map<String,Object> valuesMap = new HashMap<String,Object>(6);

    protected  static  final  List<String>  AP_RELATION_TABLE_LIST=new ArrayList<String>();

    static {
        AP_RELATION_TABLE_LIST.add("ap_indicator_relation");
        AP_RELATION_TABLE_LIST.add("ap_indicator_field_relation");
    }

    @Override
    public void setPipelineId(int pipelineId) {
        this.pipelineId = pipelineId;
    }

    /**
     * 具体数据实现方式
     * @param dataList  数据列表
     */
    protected abstract void processDetail(List<PgWalChangeAck> dataList,List<String>  cqlList);

    /**
     * 默认异常发生时候处理方式，子类可以覆盖
     * @param ex  异常种类
     */
    protected  void exceptionCaught(Exception ex)
    {
        log.error( this+" process data error!", ExceptionUtils.toStack(ex));
    }

    /**
     * 处理数据的模板方法
     * @param dataList  wal数据列表
     */
    @Override
    public void process(List<PgWalChangeAck> dataList,List<String>  cqlList) {

        try
        {
            processDetail(dataList,cqlList);
        }
        catch (Exception ex)
        {
            if (relogError) {
                exceptionCaught(ex);
            }
            throw  new MetabaseException(ex);
        }

    }

    /**
     * 初始化处理器时候需要做事情
     */
    @Override
    public void init()
    {

    }

    /**
     * 关闭处理器时候需要做的事情
     */
    @Override
    public void  close()
    {

    }


    @Override
    public  int getPipelineId()
    {
        return pipelineId;
    }

    protected void deleteObject(Long entityId)
    {
        INeo4jDataRepository dataRepository = Neo4jDataRepositoryContext.get(this.getPipelineId());
        valuesMap.clear();
        valuesMap.put("entityId",entityId);
        StringSubstitutor sub = new StringSubstitutor(valuesMap);
        String  backupAttCql= sub.replace(DELETE_FIELD_BY_HANDLE);
        dataRepository.operNeo4jDataByCqlSingleNoEx(backupAttCql,true);
        valuesMap.clear();
        valuesMap.put("entityId",entityId);
        sub = new StringSubstitutor(valuesMap);
        String  backupTableCql= sub.replace(DELETE_MODULE_BY_HANDLE);
        dataRepository.operNeo4jDataByCqlSingleNoEx(backupTableCql,true);
        valuesMap.clear();
    }

    protected   Boolean isDeleteRecord(PgWalChange walChange)
    {
        Column column =  walChange.makeOneColumn(FIELD_IS_DELETED);
        if(column==null  )
        {
            return false;
        }
        Object  columnValue=column.getRealValue();
        if(columnValue==null|| StringUtils.isBlank(String.valueOf(columnValue))|| !( columnValue  instanceof  Boolean) )
        {
            return false;
        }
        Boolean deleteFag= (Boolean) columnValue;
        column.clear();
        return deleteFag;
    }
}
