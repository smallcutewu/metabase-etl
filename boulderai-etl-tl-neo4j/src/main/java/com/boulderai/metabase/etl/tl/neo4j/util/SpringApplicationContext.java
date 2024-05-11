package com.boulderai.metabase.etl.tl.neo4j.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.stereotype.Component;

/**
 * @ClassName: SpringApplicationContext
 * @Description: ApplicationContext上下文
 * @author  df.l
 * @date 2022年09月05日
 * @Copyright boulderaitech.com
 */
@Component
public class SpringApplicationContext implements ApplicationContextAware {

    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SpringApplicationContext.applicationContext = applicationContext;
    }


    public static void close() {
        ((AbstractApplicationContext)applicationContext).close();
    }

    public static <T> T getBean(Class<T> requiredType) {
        if(applicationContext==null)
        {
            return null;
        }
        return applicationContext.getBean(requiredType);
    }


}