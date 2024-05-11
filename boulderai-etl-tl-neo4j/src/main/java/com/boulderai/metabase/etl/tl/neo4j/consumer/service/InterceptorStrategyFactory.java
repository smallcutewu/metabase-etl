package com.boulderai.metabase.etl.tl.neo4j.consumer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Service
public class InterceptorStrategyFactory {

    @Autowired
    private Map<String,InterceptorStrategy> interceptorStrategyMap;

    @Autowired
    private List<InterceptorStrategy> interceptorStrategyList;

    public InterceptorStrategy getInterceptorStratrgy(String strategy) {
        return interceptorStrategyMap.get(strategy);
    }

    public List<InterceptorStrategy> getInterceptorStrategyList() {
        return interceptorStrategyList;
    }

}
