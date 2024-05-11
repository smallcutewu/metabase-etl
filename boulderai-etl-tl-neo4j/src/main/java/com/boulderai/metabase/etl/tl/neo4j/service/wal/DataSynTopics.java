package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.lang.Constants;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;


/**
 * @ClassName: DataSynTopics
 * @Description: 数据同步主题信息
 * @author  df.l
 * @date 2022年10月27日
 * @Copyright boulderaitech.com
 */
public enum DataSynTopics {
    /**
     * 元数据同步
     */
    metabase(1,"元数据同步", DataSynConstant.MQ_EXCHANGE_VALUE,DataSynConstant.MQ_QUEUE,DataSynConstant.MQ_ROUTING_KEY),

    /**
     * 其他数据同步
     */
//    modeldata(2,"其他数据同步", DataSynConstant.MQ_EXCHANGE_MODEL_DATA_VALUE,DataSynConstant.MQ_MODEL_DATA_QUEUE,DataSynConstant.MQ_ROUTING_MODEL_DATA_KEY),

    /**
     * 其他数据同步
     */
    others(3,"其他数据同步", DataSynConstant.MQ_EXCHANGE_OTHER_DATA_VALUE,DataSynConstant.MQ_OTHER_DATA_QUEUE,DataSynConstant.MQ_ROUTING_OTHER_DATA_KEY),

    integration(5,"集成数据同步", DataSynConstant.MQ_EXCHANGE_INTEGRATION_DATA_VALUE,DataSynConstant.MQ_INTEGRATION_DATA_QUEUE,DataSynConstant.MQ_ROUTING_INTEGRATION_DATA_KEY),

    npi(5,"npi数据同步", DataSynConstant.MQ_EXCHANGE_NPI_DATA_VALUE,DataSynConstant.MQ_NPI_DATA_QUEUE,DataSynConstant.MQ_ROUTING_NPI_DATA_KEY),

    eimos(6,"eimos数据同步", DataSynConstant.MQ_EXCHANGE_EIMOS_DATA_VALUE,DataSynConstant.MQ_EIMOS_DATA_QUEUE,DataSynConstant.MQ_ROUTING_EIMOS_DATA_KEY),

    metabase_inner(7,"数据资产到同步工程", Constants.MQ_EXCHANGE_METABASE_INNER_DATA_VALUE,Constants.MQ_METABASE_INNER_DATA_QUEUE,Constants.MQ_ROUTING_METABASE_INNER_DATA_KEY),


    fail_msg(8,"批量同步失败以后重新消费", DataSynConstant.MQ_EXCHANGE_FAILMSG_DATA_VALUE,DataSynConstant.MQ_FAILMSG_DATA_QUEUE,DataSynConstant.MQ_ROUTING_FAILMSGI_DATA_KEY),

    performance_test(4,"性能测试同步", DataSynConstant.MQ_EXCHANGE_OTHER_TEST_VALUE,DataSynConstant.MQ_OTHER_TEST_QUEUE,DataSynConstant.MQ_ROUTING_OTHER_TEST_KEY),

    integration_cluster(9,"集群方式同步数据", DataSynConstant.MQ_EXCHANGE_INTEGRATION_CLUSTER_VALUE,DataSynConstant.MQ_EXCHANGE_INTEGRATION_CLUSTER_KEY,DataSynConstant.MQ_INTEGRATION_CLUSTER_QUEUE);

    private final int  type;
    private final String desc;
    private final String exchangeName;
    private final String queueName;
    private final String routingKey;

    private DataSynTopics(int  type,String desc,String exchangeName,String queueName,String routingKey)
    {
        this.type=type;
        this.desc=desc;
        this.exchangeName=exchangeName;
        this.queueName=queueName;
        this.routingKey=routingKey;
    }

    public int getType() {
        return type;
    }

    public String getDesc() {
        return desc;
    }


    public String getExchangeName() {
        return exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getRoutingKey() {
        return routingKey;
    }

}
