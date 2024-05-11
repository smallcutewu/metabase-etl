package com.boulderai.metabase.etl.starter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;


/**
 * @ClassName: SynUncaughtExceptionHandler
 * @Description: pg wal无法意料异常捕获
 * @author  df.l
 * @date 2022年11月16日
 * @Copyright boulderaitech.com
 */
public class SynUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(SynUncaughtExceptionHandler.class);

    @Override
    public  void  uncaughtException(Thread t, Throwable e) {
        try  {
            StringWriter sw =  new  StringWriter();
            e.printStackTrace( new PrintWriter(sw));
            logger.error("sync system uncaught exception {} /r/n{}",e,sw);
        }  catch  (Exception e1) {
            e1.printStackTrace();
        }


    }


}
