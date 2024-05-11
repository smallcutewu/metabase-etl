package com.boulderai.metabase.etl.tl.neo4j.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @ClassName: NamedThreadFactory
 * @Description: 线程工厂
 * @author  df.l
 * @date 2022年12月02日
 * @Copyright boulderaitech.com
 */
public class NamedThreadFactory implements ThreadFactory {
    private final AtomicInteger index = new AtomicInteger();

    private String name;

    public NamedThreadFactory(String name) {
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, this.name + "-" + index.incrementAndGet());
    }
}
