package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.etl.tl.neo4j.config.PgWalConfig;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.Column;
import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.PgWalChange;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import com.boulderai.metabase.etl.tl.neo4j.util.IntegrationDataStopper;
import com.boulderai.metabase.etl.tl.neo4j.util.SpringApplicationContext;
import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName: FileQueueManager
 * @Description: 本地文件队列管理器
 * @author  df.l
 * @date 2022年12月02日
 * @Copyright boulderaitech.com
 */
@Slf4j
public class FileQueueManager {

    private RabbitTemplate rabbitTemplate;
    private  volatile   Boolean  startOk=false;
    private final Gson gson = new Gson();
    private static final String DATA_SOURCE_COLUMN="data_source";
    private static final String EIMOS_DATA_VALUE="eimos";
    private long integrationDataCount=0L;

    private PgWalConfig pgWalConfig = SpringApplicationContext.getBean(PgWalConfig.class);

    private final ConcurrentHashMap<String,FileQueueStore> fileQueueStoreMap=new ConcurrentHashMap<String,FileQueueStore>();

    private FileQueueManager ()
    {

    }

    public  void  start()
    {
        if (startOk) {
            return;
        }

        String[] queueNames={DataSynConstant.WAL_META_QUEUE_NAME,DataSynConstant.WAL_EIMOS_MODEL_DATA_QUEUE_NAME,
                DataSynConstant.WAL_INTEGRATION_MODEL_DATA_QUEUE_NAME, DataSynConstant.WAL_NPI_MODEL_DATA_QUEUE_NAME
        ,   DataSynConstant.WAL_OTHER_MODEL_DATA_QUEUE_NAME  };
        Integer[] threadCounts={1,1,1, 1,2};
        for (int i=0;i< queueNames.length; i++) {
            String queueName=queueNames[i];
            FileQueueStore  fileQueueStore=new FileQueueStore( queueName, rabbitTemplate,threadCounts[i]);
            fileQueueStoreMap.put(queueName, fileQueueStore);
        }

        if(fileQueueStoreMap.size()==queueNames.length)
        {
            startOk=true;
//            if(queueNames.length==3)
//            {
//                PerformanceTestThread  th=new PerformanceTestThread(1,DataSynConstant.WAL_PERFORMANCE_TEST_QUEUE_NAME);
//                th.start();
//            }
            log.info("pg_wal_local_queue  start ok!");
        }
    }

    private class  PerformanceTestThread extends Thread {
        private int id=0;
        private String queueName;
        public PerformanceTestThread(int id,String queueName)
        {
          this.id=id;
            this.queueName=queueName;
        }

        public void run() {
//            FileQueueStore  fileQueueStore=    fileQueueStoreMap.get(queueName);
//
//            String  testStr=" {\"kind\":\"insert\",\"schema\":\"public\",\"table\":\"da_entity_attribute\",\"columnnames\":[\"id\",\"name\",\"code\",\"alias\",\"description\",\"entity_id\",\"data_type\",\"primary_key\",\"is_unique\",\"___mandatory__1\",\"column_name\",\"manage_type\",\"data_catalog\",\"properties\",\"ref_entity_id\",\"namespace_id\",\"status\",\"created_at\",\"updated_at\",\"created_by\",\"updated_by\",\"mandatory_condition\",\"enum_values\",\"data_scale\",\"biz_define\",\"org\",\"come_from\",\"ref_type\",\"ref_attr_id\",\"ref_notes\",\"tags\",\"data_length\",\"display_ref_attr_ids\",\"ref_entity_name\",\"ref_attr_name\",\"display_ref_attr_names\",\"options\",\"item_type\",\"ref_namespace_id\",\"filter_value\",\"display_attr_code\",\"components_code\",\"namespace_code\",\"filter_field\",\"prefilter\",\"biz_primary_key\",\"api_data_type\",\"ref_choice\",\"app_scene\",\"is_serial_number\",\"prefix\",\"timestamp_type\",\"random_length\",\"table_type\",\"permission_attr\",\"___is_system_attr__1\",\"is_system\",\"default_value\",\"is_editable\",\"required\",\"lineage\",\"restriction\"],\"columntypes\":[\"bigint\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"character varying(4000)\",\"bigint\",\"character varying(100)\",\"boolean\",\"boolean\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"jsonb\",\"bigint\",\"bigint\",\"integer\",\"timestamp(6) without time zone\",\"timestamp(6) without time zone\",\"character varying(100)\",\"character varying(100)\",\"character varying(4000)\",\"character varying(4000)\",\"character varying(10)\",\"character varying(4000)\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"bigint\",\"character varying(4000)\",\"text[]\",\"integer\",\"text[]\",\"character varying(100)\",\"character varying(100)\",\"text[]\",\"jsonb\",\"character varying(100)\",\"bigint\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"character varying(100)\",\"boolean\",\"character varying(100)\",\"character varying(100)\",\"text[]\",\"boolean\",\"character varying(100)\",\"integer\",\"integer\",\"smallint\",\"boolean\",\"boolean\",\"boolean\",\"text\",\"boolean\",\"character varying(100)\",\"jsonb\",\"character varying(255)\"],\"columnvalues\":[\"1049198318454665216\",\"属性生成引用\",\"field_from\",null,null,\"1031491277873000448\",\"text\",\"false\",\"false\",null,\"field_from\",\"undefined\",null,null,null,\"1024592630954872832\",\"1\",\"2022-12-05 13:39:32.281\",\"2022-12-05 13:39:32.281\",\"00081\",\"00081\",null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,\"tech\",null,null,\"false\",null,null,null,\"false\",null,\"1\",null,\"1\",\"false\",\"false\",\"false\",null,\"true\",null,null,null]}\n";
//            for (long k=0;k<Long.MAX_VALUE-10000; k++) {
//              String data= queueName+" ## "+id+" ## "+k +" ## "+testStr;
//                fileQueueStore.sendProduction(data);
//            }
        }
    }



    public String printTotalPerfile()
    {
        StringBuilder  sb=new StringBuilder("元数据消费统计:<br/>");
        FileQueueStore  fileQueueStore=  fileQueueStoreMap.get(DataSynConstant.WAL_META_QUEUE_NAME);
        sb.append(fileQueueStore.printConsumeStat());

        sb.append("<br/>######################################<br/>数据消费统计:<br/>");
        fileQueueStore=  fileQueueStoreMap.get(DataSynConstant.WAL_MODEL_DATA_QUEUE_NAME);
        sb.append(fileQueueStore.printConsumeStat());

        sb.append("<br/>######################################<br/>性能测试统计:<br/>");
        fileQueueStore=  fileQueueStoreMap.get(DataSynConstant.WAL_PERFORMANCE_TEST_QUEUE_NAME);
        if (fileQueueStore != null) {
            sb.append(fileQueueStore.printConsumeStat());
        }
        return  sb.toString();

    }

    public void clearStatLog()
    {
        Set<Map.Entry<String,FileQueueStore>> entrySet=fileQueueStoreMap.entrySet();
        for (Map.Entry<String, FileQueueStore> entry : entrySet) {
            FileQueueStore  store=   entry.getValue();
            if (store!=null) {
                store.clearStat();
            }
        }
    }

    private void sendProduct(PgWalChange change, String queueName, boolean isDeleteRec)
    {
        FileQueueStore  fileQueueStore=   null;
        String data = gson.toJson(change);

        //如果是模型数据则开始细分队列名称
        if(DataSynConstant.WAL_MODEL_DATA_QUEUE_NAME.equals(queueName))
        {
            Column column= change.makeOneColumn(DATA_SOURCE_COLUMN);
            //做业务中台用的api或其他eimos
            if(column != null && EIMOS_DATA_VALUE.equals(   column.getRealValue()))
            {
                String tableName=change.getTable();
                if(StringUtils.isNotBlank(tableName))
                {
                    if (tableName.contains(DataSynConstant.WAL_META_NPI_TABLE)) {
                        fileQueueStore= fileQueueStoreMap.get(DataSynConstant.WAL_NPI_MODEL_DATA_QUEUE_NAME);
                        fileQueueStore.sendProduction(data,false);
                    }
                    else
                    {
                        fileQueueStore= fileQueueStoreMap.get(DataSynConstant.WAL_EIMOS_MODEL_DATA_QUEUE_NAME);
                        fileQueueStore.sendProduction(data,isDeleteRec);
                    }
                }
            }
            else
            {
                //集成系统用的
                if( IntegrationDataStopper.isStart())
                {
                    // 集成数据走集群消费
                    if(pgWalConfig.getClusterSwitch()) {
                        rabbitTemplate.convertAndSend( DataSynTopics.integration_cluster.getExchangeName(),  DataSynTopics.integration_cluster.getQueueName(), data);
                    } else {
                        fileQueueStore = fileQueueStoreMap.get(DataSynConstant.WAL_INTEGRATION_MODEL_DATA_QUEUE_NAME);
                        fileQueueStore.sendProduction(data, isDeleteRec);
                    }
                }
                else
                {
                    integrationDataCount++;
                    if(integrationDataCount%1000==0)
                    {
                        log.info(" Integration Model Data ignor to save! ");
                    }
                    if(integrationDataCount>=Integer.MAX_VALUE)
                    {
                        integrationDataCount=1;
                    }
                }

            }

        }
        else  if(DataSynConstant.WAL_META_QUEUE_NAME.equals(queueName))
        {
            fileQueueStore= fileQueueStoreMap.get(DataSynConstant.WAL_META_QUEUE_NAME);
            fileQueueStore.sendProduction(data,false);
        }
        else{
            fileQueueStore= fileQueueStoreMap.get(DataSynConstant.WAL_OTHER_MODEL_DATA_QUEUE_NAME);
            fileQueueStore.sendProduction(data,false);
        }

        data=null;
    }


    public void sendProduct(PgWalChange  change,String queueName)
    {
        FileQueueStore  fileQueueStore=   null;
        String data = gson.toJson(change);

        boolean isDeleteRec=false;
//        if (change.getKind()!=null&&change.getKind().equals(EventType.DELETE.toString())) {
//            isDeleteRec=true;
//        }
        try{
            sendProduct(  change, queueName,  isDeleteRec);
        }
        catch (Exception  ex)
        {
            log.error("send product  file error!",ex);
        }
        data=null;
    }

    public void close() {
//      for(  Map.Entry<String, FileQueueStore> entry : fileQueueStoreMap.entrySet())
//      {
//          entry.getValue().close();
//      }
//        fileQueueStoreMap.clear();
    }


    public Boolean getStartOk() {
        return startOk;
    }

    public FileQueueStore getFileQueueStore(String groupId) {
        return fileQueueStoreMap.get(groupId);
    }

    public void setRabbitTemplate(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    private  static  final   class Holder
    {
        private  static final FileQueueManager INSTANCE =new FileQueueManager();
    }

    public  static FileQueueManager  getInstance()
    {
        return  Holder.INSTANCE;
    }
}
