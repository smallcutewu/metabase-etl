package com.boulderai.metabase.etl.tl.neo4j.util;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.HStoreConverter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: PostgresqlWalUtil
 * @Description: wal提取日志工具
 * @author  df.l
 * @date 2022年09月05日
 * @Copyright boulderaitech.com
 */
public class PostgresqlWalUtil {
    private static Pattern NUMERICREGEX = Pattern.compile("[.0-9]+");
    private static final ObjectMapper MAPPER = new ObjectMapper();



    public static AtomicLong getSeqCounter() {
        return SeqCounterHolder.INSTANCE;
    }

//    public static String hstoreToJson(String value){
//        try {
//            Map map = HStoreConverter.fromString(value);
//            return mapper.writeValueAsString(map);
//        } catch (JsonProcessingException e) {
//            return null;
//        }
//    }

    public static class SeqCounterHolder {
        private static final AtomicLong INSTANCE = new AtomicLong(0);
    }

    public static String extractNumeric(String str){
        Matcher m = NUMERICREGEX.matcher(str);
        if (m.find()){
            return m.group(0);
        }else{
            return null;
        }
    }
}
