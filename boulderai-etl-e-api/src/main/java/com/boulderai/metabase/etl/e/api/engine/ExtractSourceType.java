package com.boulderai.metabase.etl.e.api.engine;

import java.lang.annotation.*;

/**
 * @author wanganbang 当前cdc时间发起的拉取类型
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented

public @interface ExtractSourceType {

    SourceTypeEnum type() default SourceTypeEnum.PG;
}
