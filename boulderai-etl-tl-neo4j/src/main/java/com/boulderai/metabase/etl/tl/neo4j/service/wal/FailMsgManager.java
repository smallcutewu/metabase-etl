package com.boulderai.metabase.etl.tl.neo4j.service.wal;

import com.boulderai.metabase.lang.util.SleepUtil;
import com.boulderai.metabase.etl.tl.neo4j.util.DataSynConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Component
@Slf4j
public class FailMsgManager {
    private ExecutorService executorService;
    private LinkedBlockingDeque<String> cqlQueue=new LinkedBlockingDeque<String>();
    private long maxSqlCount=50000;


    @Autowired
    private RabbitTemplate rabbitTemplate;

    private ScheduledExecutorService checkSmallMsgService;

    private DataSynTopics[] checkTopics={ DataSynTopics.eimos,DataSynTopics.integration};

    private static final String  SMALL_TEST_MSG="{ " +
            "    \"kind\": \"time_test\", " +
            "    \"schema\": \""+DataSynConstant.METABASE_WAL_SCHEMA+"\", " +
            "    \"table\": \""+DataSynConstant.METABASE_WAL_SCHEMA+"\", " +
            "    \"oldkeys\": { " +
            "      \"keynames\": [\"id\"], " +
            "      \"keytypes\": [\"bigint\"], " +
            "      \"keyvalues\": [4] " +
            "    } " +
            "  }";
    @PostConstruct
    public  void init()
    {
        executorService= Executors.newFixedThreadPool(1);
        executorService.submit(new SendSql2MqRunnable());
        checkSmallMsgService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "checkSmallMsgthread");
            // 设置线程为守护线程，主线程退出，子线程也随之退出
            t.setDaemon(true);
            return t;
        });

        checkSmallMsgService.scheduleWithFixedDelay(new CheckSmallMsgRunnable(),1,2, TimeUnit.MINUTES);
    }

    protected class  CheckSmallMsgRunnable implements  Runnable {
        @Override
        public void run() {
            for(DataSynTopics  dataSynTopics:  checkTopics)
            {
                try
                {
                    rabbitTemplate.convertAndSend( dataSynTopics.getExchangeName(),  dataSynTopics.getQueueName(), SMALL_TEST_MSG);
                }
                catch (Exception ex)
                {

                }

            }
        }
    }


    public void addMutilCql(List<String>  cqlList)
    {
        cqlQueue.addAll(cqlList);
    }

    public void addOneCql(String  cql)
    {
        cqlQueue.add(cql);
    }


    class SendSql2MqRunnable  implements Runnable{

        @Override
        public void run() {
            List<String> sqlList=new ArrayList<String>();
            while (true) {
                if(!cqlQueue.isEmpty())
                {
                    sqlList.clear();
                    cqlQueue.drainTo(sqlList,100);
                    if(!sqlList.isEmpty())
                    {
                        for (String sql : sqlList) {
                            try
                            {
                                rabbitTemplate.convertAndSend( DataSynConstant.MQ_EXCHANGE_FAILMSG_DATA_VALUE, DataSynConstant.MQ_FAILMSG_DATA_QUEUE, sql);
                            }
                            catch (Exception ex)
                            {
                                log.error(" rabbitTemplate.convertAndSend  error! sql:"+ sql,ex);
                            }

                        }
                    }
                    else
                    {
                        SleepUtil.sleepSecond(1);
                    }
                }
                else
                {
                    SleepUtil.sleepSecond(3);
                }
            }
        }
    }
}
