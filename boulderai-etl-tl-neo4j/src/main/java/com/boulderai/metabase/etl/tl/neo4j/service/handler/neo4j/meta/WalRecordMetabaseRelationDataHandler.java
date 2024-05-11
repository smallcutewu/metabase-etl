package com.boulderai.metabase.etl.tl.neo4j.service.handler.neo4j.meta;

import com.alibaba.druid.sql.dialect.postgresql.ast.expr.PGExtractExpr;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.*;
import com.boulderai.metabase.etl.tl.neo4j.util.SpringApplicationContext;
import com.boulderai.metabase.lang.Constants;
import com.boulderai.metabase.etl.tl.neo4j.config.LogicEntityConfig;
import com.boulderai.metabase.etl.tl.neo4j.config.TableToBeanConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.handler.BaseDataHandler;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.INeo4jDataRepository;
import com.boulderai.metabase.etl.tl.neo4j.service.neo4j.Neo4jDataRepositoryContext;
import com.boulderai.metabase.etl.tl.neo4j.service.parser.RelationCqlParser;
import com.boulderai.metabase.etl.tl.neo4j.service.parser.RelationCqlParserFactory;
import com.boulderai.metabase.lang.er.RelationType;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.Neo4jSyncContext;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.etl.tl.neo4j.util.DbOperateUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: PersistMetabaseDataHandler
 * @Description: 配置节点关系持久化处理器
 * @author  df.l
 * @date 2022年10月12日
 * @Copyright boulderaitech.com
 */
public class WalRecordMetabaseRelationDataHandler extends BaseDataHandler {

    private final List<String>  relationCqlList=  new ArrayList<String>();

    @Override
    public void  close()
    {
        relationCqlList.clear();
    }

    @Override
    protected void processDetail(List<PgWalChangeAck> dataList, List<String>  cqlList) {
        Neo4jSyncContext neo4jSyncContext =Neo4jSyncContext.getInstance();
        relationCqlList.clear();
//        StringBuilderPool   pool=StringBuilderPool.getInstance();
        INeo4jDataRepository dataRepository =  Neo4jDataRepositoryContext.get(this.getPipelineId());
        for (PgWalChangeAck changeAck : dataList) {
            PgWalChange change=changeAck.getChange();
            String tableName = change.getTable();

            List<TableToBeanConfig> tableToBeanConfigs=neo4jSyncContext.getTableToBeanRelationConfigByTableName(tableName);
            if(CollectionUtils.isEmpty(tableToBeanConfigs))
            {
                continue;
            }

            if (tableName.equals(DA_ENTITY_ATTRIBUTE_TABLE))
            {
                Column column=  change.makeOneColumn(ENTITY_ID_COLUMN);
                if(column!=null&&column.getRealValue()!=null)
                {
                    Long entityId= (Long) column.getRealValue();
                    String realTableName=  neo4jSyncContext.getLogicEntityTableName(entityId);
                    if(realTableName!=null&&realTableName.endsWith(Constants.VERSION_TABLE_SUB))
                    {
                        continue;
                    }
                }
            }

            if (tableName.equals(DA_LOGIC_ENTITY_TABLE))
            {
                Column column=  change.makeOneColumn(TABLE_NAME_COLUMN);
                if(column!=null&&column.getRealValue()!=null)
                {
                    String realTableName= (String) column.getRealValue();
                    if(realTableName!=null&&realTableName.endsWith(Constants.VERSION_TABLE_SUB))
                    {
                        continue;
                    }
                }
            }

            AbstractRowEvent row= change.getChangeToRow();
            EventType  eventType=row.getEventType();

               //找出此表所有的关系字段
               for(TableToBeanConfig beanConfig : tableToBeanConfigs )
               {
                   EventType  tempEventType=eventType;

                   if (isDeleteRecord( change)) {
                       tempEventType=EventType.DELETE;
                   }

                   if(!tempEventType.equals(EventType.DELETE))
                   {
                       //软删除处理，将status字段改成0,1
                       if(StringUtils.isNotBlank(beanConfig.getDeleteColumn())
                               &&  StringUtils.isNotBlank(beanConfig.getDeleteColumnValue()))
                       {
                           Column  deleteColumn=  change.makeOneColumn(beanConfig.getDeleteColumn());
                           if(deleteColumn!=null)
                           {
                               Object  deleteRecordValue=deleteColumn.getRealValue();
                               if(beanConfig.getDeleteColumnValue().equals(deleteRecordValue)&&
                                       tempEventType.equals(EventType.UPDATE))
                               {
                                   tempEventType=EventType.DELETE;
                               }
                           }

                       }
                   }




                   //找出此表此列所有的关系同步对象信息
                   String  relationName=beanConfig.getRelationName();
                   RelationType  relationType =RelationType.getRelationTypeByName(relationName);
                   RelationCqlParser  relationCqlParser= RelationCqlParserFactory.getInstance().getRelationCqlParser(relationType);
                   if(relationCqlParser!=null)
                   {
                       String cql=null;
                      switch (tempEventType)
                      {
                          case INSERT:
                              cql=relationCqlParser.parseInsertCql(beanConfig ,  change );
                              break;
                          case UPDATE:
                              cql=relationCqlParser.parseUpdateCql(beanConfig ,  change );
                              break;
                          case DELETE:
                              cql=relationCqlParser.parseDeleteCql(beanConfig ,  change );
                              break;
                          default :
                      }

                       switch (tempEventType)
                       {
                           case INSERT:
                           case UPDATE:
                               if(relationCqlParser.deleteOnUpsert())
                               {
                                   String deleteCql=relationCqlParser.parseDeleteCql(beanConfig ,  change );
                                   if(StringUtils.isNotBlank(deleteCql)&& neo4jSyncContext.getExtractEngineRunning())
                                   {
                                       dataRepository.operNeo4jDataByCqlSingleNoEx(deleteCql,true);
                                   }
                               }

                               break;
                           default :
                       }

                      if (StringUtils.isNotBlank(cql)) {
                          relationCqlList.add(cql);
                          if (DA_LOGIC_RELATION_TABLE.equals(tableName)) {
                              Column startTableColumn=  change.makeOneColumn(START_ID_COLUMN);
                              removeTableRelationCache( startTableColumn);
                              Column endTableColumn=  change.makeOneColumn(END_ID_COLUMN);
                              removeTableRelationCache( endTableColumn);
                          }
                      }
                   }
               }
        }



        if(CollectionUtils.isNotEmpty(relationCqlList) && neo4jSyncContext.getExtractEngineRunning())
        {
            for(String  cql : relationCqlList)
            {
                dataRepository.operNeo4jDataByCqlSingle(cql);
            }
        }
    }

    private void removeTableRelationCache(Column tableColumn)
    {
        if(tableColumn!=null&&tableColumn.getRealValue()!=null)
        {
            Neo4jSyncContext neo4jSyncContext =Neo4jSyncContext.getInstance();
            Long tableId= (Long) tableColumn.getRealValue();
            LogicEntityConfig tableConfig= neo4jSyncContext.getLogicEntityById(tableId);

            if(tableConfig==null)
            {
                List<LogicEntityConfig> logicEntityConfigList = DbOperateUtil.queryBeanList(DataSynConstant.DA_LOGIC_ENTITY_CONFIG_BY_TABLE_ID_SQL,LogicEntityConfig.class,tableId);
                if(CollectionUtils.isNotEmpty(logicEntityConfigList))
                {
                    tableConfig=logicEntityConfigList.get(0);
                }
            }

            if (tableConfig != null) {
                String tableName=tableConfig.getTableName();
                neo4jSyncContext.removeModelDataConfigRelationByTable(tableName, DataSynConstant.ERTYPE_MUTIL);
            }

        }
    }

    @Override
    public String name() {
        return RECORD_RELATION_DATA_HANDLER;
    }
}
