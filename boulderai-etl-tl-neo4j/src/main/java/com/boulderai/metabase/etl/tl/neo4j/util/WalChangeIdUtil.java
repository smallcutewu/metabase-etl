package com.boulderai.metabase.etl.tl.neo4j.util;

import java.util.concurrent.atomic.AtomicLong;
/**
 * @ClassName: WalChangeIdUtil
 * @Description: wal日志id生成器
 * @author  df.l
 * @date 2022年10月18日
 * @Copyright boulderaitech.com
 */
public class WalChangeIdUtil {
    private  static  AtomicLong changeIdFlag=new AtomicLong(1L);
    private static  final Long  MAX_CHANGE_ID=9999999L ;
    public static Long  generalChangeId()
    {
        Long cid=changeIdFlag.addAndGet(1);
        if(cid>=MAX_CHANGE_ID)
        {
            synchronized (WalChangeIdUtil.class)
            {
                if(cid>=MAX_CHANGE_ID)
                {
                    changeIdFlag.set(1L);
                    cid=changeIdFlag.get();
                }
            }
        }
        return cid;
    }

    /**
     * 此方法仅配合单元测试使用
     * @param changeIdFlag
     */
    public static void setChangeIdFlag(AtomicLong changeIdFlag) {
        WalChangeIdUtil.changeIdFlag = changeIdFlag;
    }
}
