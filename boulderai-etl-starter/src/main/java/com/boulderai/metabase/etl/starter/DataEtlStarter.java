package com.boulderai.metabase.etl.starter;

import com.boulderai.common.core.annotation.EnableCustomSwagger2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: DataSyncApplication
 * @Description: pg wal数据同步应用入口
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
@SpringBootApplication
@EnableDiscoveryClient
@EnableCustomSwagger2
@ComponentScan("com.boulderai.metabase.etl")
@EnableScheduling
public class DataEtlStarter{
    private static final Logger logger = LoggerFactory.getLogger(DataEtlStarter.class);
    public static final String THREAD_NAME_METABASE_SYNC_SERVER= "metabase-sync-server";

    private static String[] args;

    private static ConfigurableApplicationContext context;

    public static void main(String[] args) {
        Thread.currentThread().setName(THREAD_NAME_METABASE_SYNC_SERVER);
        Thread.currentThread().setUncaughtExceptionHandler( new SynUncaughtExceptionHandler());
        DataEtlStarter.args = args;
        context = SpringApplication.run(DataEtlStarter.class);
        logger.info("etl stared!");
    }

    public static String restart(){

        ExecutorService threadPool = new ThreadPoolExecutor(1,1,0, TimeUnit.SECONDS,new ArrayBlockingQueue<>( 1 ),new ThreadPoolExecutor.DiscardOldestPolicy ());
        threadPool.execute (()->{
            context.close ();
            context = SpringApplication.run ( DataEtlStarter.class,args );
        } );
        threadPool.shutdown ();
        return "etl reStared!";
    }


}
