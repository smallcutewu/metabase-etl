//package com.boulderai.metabase.sync.core.service.handler.neo4j;
//
//import com.boulderai.metabase.sync.core.config.TableToBeanConfig;
//import com.boulderai.metabase.sync.core.service.handler.BaseDataHandler;
//import com.boulderai.metabase.sync.core.service.neo4j.Neo4jDataRepository;
//import com.boulderai.metabase.sync.core.service.neo4j.Neo4jDataRepositoryContext;
//import com.boulderai.metabase.sync.core.service.parser.RelationCqlParser;
//import com.boulderai.metabase.sync.core.service.parser.RelationCqlParserFactory;
//import com.boulderai.metabase.lang.er.RelationType;
//import com.boulderai.metabase.sync.core.service.wal.Neo4jSyncContext;
//import com.boulderai.metabase.sync.core.service.wal.model.AbstractRowEvent;
//import com.boulderai.metabase.sync.core.service.wal.model.Column;
//import com.boulderai.metabase.sync.core.service.wal.model.EventType;
//import com.boulderai.metabase.sync.core.service.wal.model.PgWalChange;
//import com.boulderai.metabase.sync.core.util.pool.stringbuffer.StringBuilderPool;
//import org.apache.commons.collections4.CollectionUtils;
//import org.apache.commons.lang3.StringUtils;
//
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * @ClassName: PersistAdapterDataHandler
// * @Description: 配置节点关系持久化处理器
// * @author  df.l
// * @date 2022年10月12日
// * @Copyright boulderaitech.com
// */
//public class WalRecordRelationAdapterDataHandler extends BaseDataHandler {
//
//    private final List<String>  relationCqlList=  new ArrayList<String>();
//
//    @Override
//    public void  close()
//    {
//        relationCqlList.clear();
//    }
//
//    @Override
//    protected void processDetail(List<PgWalChange> dataList) {
//        Neo4jSyncContext neo4jSyncContext =Neo4jSyncContext.getInstance();
//        relationCqlList.clear();
//        StringBuilderPool   pool=StringBuilderPool.getInstance();
//
//        for (PgWalChange change : dataList) {
//            String tableName = change.getTable();
//
//            List<TableToBeanConfig> tableToBeanConfigs=neo4jSyncContext.getTableToBeanRelationConfigByTableName(tableName);
//            if(CollectionUtils.isEmpty(tableToBeanConfigs))
//            {
//                continue;
//            }
//
//            AbstractRowEvent row= change.getChangeToRow();
//            EventType  eventType=row.getEventType();
//
//               //找出此表所有的关系字段
//               for(TableToBeanConfig beanConfig : tableToBeanConfigs )
//               {
//                   EventType  tempEventType=eventType;
//
//                   //软删除处理，将status字段改成0,1
//                   if(StringUtils.isNotBlank(beanConfig.getDeleteColumn())
//                       &&  StringUtils.isNotBlank(beanConfig.getDeleteColumnValue()))
//                   {
//                       Column  deleteColumn=  change.makeOneColumn(beanConfig.getDeleteColumn());
//                       if(deleteColumn!=null)
//                       {
//                           Object  deleteRecordValue=deleteColumn.getRealValue();
//                           if(beanConfig.getDeleteColumnValue().equals(deleteRecordValue)&&
//                                   tempEventType.equals(EventType.UPDATE))
//                           {
//                               tempEventType=EventType.DELETE;
//                           }
//                       }
//
//                   }
//
//                   //找出此表此列所有的关系同步对象信息
//                   String  relationName=beanConfig.getRelationName();
//                   RelationType  relationType =RelationType.getRelationTypeByName(relationName);
//                   RelationCqlParser  relationCqlParser= RelationCqlParserFactory.getInstance().getRelationCqlParser(relationType);
//                   if(relationCqlParser!=null)
//                   {
//                       String cql=null;
//                      switch (tempEventType)
//                      {
//                          case INSERT:
//                              cql=relationCqlParser.parseInsertCql(beanConfig ,  change );
//                              break;
//                          case UPDATE:
//                              cql=relationCqlParser.parseUpdateCql(beanConfig ,  change );
//                              break;
//                          case DELETE:
//                              cql=relationCqlParser.parseDeleteCql(beanConfig ,  change );
//                              if(relationCqlParser.needParseModelData())
//                              {
//                                 String deleteModelDataCql=relationCqlParser.parseDeleteModelDataCql(beanConfig ,  change);
//                                 if(StringUtils.isNotBlank(deleteModelDataCql))
//                                 {
//                                     relationCqlList.add(deleteModelDataCql);
//                                 }
//                              }
//                              break;
//                          default :
//                      }
////                      switch (tempEventType)
////                      {
////                           case INSERT:
////                           case UPDATE:
////                               if(relationCqlParser.deleteOnUpsert())
////                               {
////                                   String deleteCql=relationCqlParser.parseDeleteCql(beanConfig ,  change);
////                                   if(StringUtils.isNotBlank(deleteCql))
////                                   {
////                                       dataRepository.operNeo4jDataByCqlSingleNoEx(deleteCql);
////                                   }
////                               }
////                               break;
////                           default :
////                       }
//
//                      if (StringUtils.isNotBlank(cql)) {
//                          relationCqlList.add(cql);
//                      }
//                   }
//               }
//        }
//
//
//
//        if(CollectionUtils.isNotEmpty(relationCqlList))
//        {
//            Neo4jDataRepository dataRepository =  Neo4jDataRepositoryContext.get(this.getPipelineId());
//            for(String  cql : relationCqlList)
//            {
//                dataRepository.operNeo4jDataByCqlSingle(cql,true);
//            }
//        }
//    }
//
//    @Override
//    public String name() {
//        return RECORD_RELATION_DATA_HANDLER;
//    }
//}
