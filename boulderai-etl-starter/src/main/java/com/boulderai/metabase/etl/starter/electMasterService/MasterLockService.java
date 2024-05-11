package com.boulderai.metabase.etl.starter.electMasterService;

import cn.hutool.core.util.ObjectUtil;
import com.boulderai.metabase.etl.e.engine.PgExtractEngine;
import com.boulderai.metabase.etl.starter.electMasterService.util.NetUtil;
import com.boulderai.metabase.lang.exception.MetabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class MasterLockService {

    private  final static Logger logger = LoggerFactory.getLogger(MasterLockService.class);

    public static final String MASTER_LOCK = "MASTER_LOCK_HOST";

    public static String LOCALHOST_IP_PORT;

    @Autowired
    private PgExtractEngine pgExtractEngine;

    @Autowired
    private RedisTemplate<String,String> redisTemplate;

    public Boolean lock() {
        Boolean result = redisTemplate.opsForValue().setIfAbsent(MASTER_LOCK, LOCALHOST_IP_PORT);
        if (result == true) {
            logger.info("选举锁已拿到");
            pgExtractEngine.create();
            return result;
        } else {
            logger.info("选举锁被其他节点持有");
            return result;
        }
    }

    public boolean concurrentHasLock() {
        String MASTER_HOST = redisTemplate.opsForValue().get(MASTER_LOCK);
        return LOCALHOST_IP_PORT.equals(MASTER_HOST);
    }

    public boolean unlock() {
        if(ObjectUtil.isEmpty(redisTemplate.opsForValue().get(MASTER_LOCK))){
            logger.info("当前锁不存在，解锁失败");
            return false;
        }
        if (concurrentHasLock()) {
            Boolean unlockResult = redisTemplate.delete(MASTER_LOCK);
            if (Boolean.FALSE.equals(unlockResult)) {
                throw new MetabaseException("redis解锁，失败");
            }
            logger.info("选举锁已释放");
            pgExtractEngine.closeEngine();
            return true;
        } else {
            logger.info("当前节点未持有锁，解锁失败");
            return false;
        }
    }

    public boolean forceUnlock() {

        Boolean unlockResult = redisTemplate.delete(MASTER_LOCK);
        if (Boolean.FALSE.equals(unlockResult)) {
            throw new MetabaseException("redis解锁，失败");
        }
        logger.info("选举锁已释放");
        pgExtractEngine.closeEngine();
        return true;
    }

    public String getMasterLock() {
        return redisTemplate.opsForValue().get(MASTER_LOCK);
    }

    @PostConstruct
    public void initParam() {
        // 初始化参数；
        LOCALHOST_IP_PORT = NetUtil.getLocalIpAddr().get(0);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> unlock()));

    }



}
