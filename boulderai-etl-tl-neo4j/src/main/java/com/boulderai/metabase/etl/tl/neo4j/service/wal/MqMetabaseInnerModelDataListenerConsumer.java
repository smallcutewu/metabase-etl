//package com.boulderai.metabase.sync.core.service.wal;
//
//import com.boulderai.metabase.lang.Constants;
//import com.boulderai.metabase.lang.sql.JdbcSql;
//import com.boulderai.metabase.lang.util.SleepUtil;
//import com.boulderai.metabase.sync.core.service.wal.model.PgWalChange;
//import com.boulderai.metabase.sync.core.util.DataSynConstant;
//import com.boulderai.metabase.sync.core.util.DbOperateUtil;
//import com.boulderai.metabase.sync.core.util.StartFlagger;
//import com.boulderai.metabase.sync.core.util.Stopper;
//import com.google.common.cache.CacheBuilder;
//import com.google.common.cache.CacheLoader;
//import com.google.common.cache.LoadingCache;
//import com.rabbitmq.client.Channel;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.collections4.CollectionUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.springframework.amqp.core.ExchangeTypes;
//import org.springframework.amqp.rabbit.annotation.Exchange;
//import org.springframework.amqp.rabbit.annotation.Queue;
//import org.springframework.amqp.rabbit.annotation.QueueBinding;
//import org.springframework.amqp.rabbit.annotation.RabbitListener;
//import org.springframework.amqp.support.AmqpHeaders;
//import org.springframework.core.annotation.Order;
//import org.springframework.messaging.handler.annotation.Header;
//import org.springframework.stereotype.Component;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//
//@Component
//@Order(Integer.MAX_VALUE-110)
//@Slf4j
//public class MqMetabaseInnerModelDataListenerConsumer extends MqModelDataListenerConsumer{
//
//
//    private final LoadingCache<String, Map<String,String>> allTableColumnMapCache = CacheBuilder.newBuilder()
//            .maximumSize(100).expireAfterAccess(DataSynConstant.ONE_HOUR_SECONDS, TimeUnit.SECONDS)
//            .recordStats()
//            .build(
//                    new CacheLoader<String, Map<String,String>>() {
//                        @Override
//                        public Map<String,String> load(String key) throws Exception {
//                            String  schemaTable[]=key.split("#");
//                            String schema=schemaTable[0];
//                            String table=schemaTable[1];
//                            Map<String,String>  columnMap=new HashMap<String,String>();
//                            List<Map<String, Object>> pkMapList = DbOperateUtil.queryMapList(DataSynConstant.TABLE_all_COLUMN_SQL,table,schema);
//                            if(CollectionUtils.isNotEmpty(pkMapList))
//                            {
//                                for (Map<String, Object>  map: pkMapList) {
//
////                                    String tableSchema= (String) map.get("table_schema");
////                                    String tableName= (String) map.get("table_name");
//                                    String columnName= (String) map.get("field");
//                                    String columnType= (String) map.get("type");
//                                    columnMap.put(columnName,columnType);
//                                    map.clear();
//                                }
//                            }
//                            pkMapList.clear();
//                            return  columnMap;
//                        }
//                    }
//            );
//
//
//    public MqMetabaseInnerModelDataListenerConsumer()
//    {
//        this.baseCount=10;
//    }
//
//
//    /**
//     * 消费模型元数据队列信息建立模型节点记忆关系
//     * @param    data
//     * @param   deliveryTag
//     * @param   channel
//     * autoDelete = "true"
//     */
//    @RabbitListener(bindings = @QueueBinding(value = @Queue(value = Constants.MQ_METABASE_INNER_DATA_QUEUE, durable = "true"),
//            exchange = @Exchange(value = Constants.MQ_EXCHANGE_METABASE_INNER_DATA_VALUE, type = ExchangeTypes.FANOUT), key = Constants.MQ_ROUTING_METABASE_INNER_DATA_KEY), ackMode = "MANUAL"
//            ,concurrency =  "1"
//    )
//    public void consumerPgWalChange(String data, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag, Channel channel)
//    {
//        //其他组件没有准备好，请稍等
//        if(!Stopper.isRunning()|| !StartFlagger.isOK())
//        {
////
//            SleepUtil.sleepSecond(10);
//            return  ;
//        }
//
//        if(StringUtils.isBlank(data))
//        {
//            onMsgAck(true, data,   deliveryTag,  channel);
//            return  ;
//        }
//
//        JdbcSql record =null;
//        try {
//            record = gson.fromJson(data, JdbcSql.class);
//        }
//        catch(Exception ex)
//        {
//            log.error("gson fromJson error!",ex);
//        }
//        if(record==null)
//        {
//            onMsgAck(true, data,   deliveryTag,  channel);
//            return  ;
//        }
//        PgWalChange  change=  convertJdbcSql2PgWalChange( record );
//    }
//
//
//    private  PgWalChange   convertJdbcSql2PgWalChange(JdbcSql record )
//    {
//        PgWalChange  change=new PgWalChange();
//        switch (record.getMethod())
//        {
//            case Constants.METHODS_INSERT:
//            case Constants.METHODS_INSERT_BATCH:
//                break;
//            case Constants.METHODS_UPDATE:
//                break;
//
//        }
//
//        return change;
//    }
//
//
//    @Override
//    public  ConsumerType  getConsumerType()
//    {
//        return ConsumerType.metabase_inner;
//    }
//
//
//}
