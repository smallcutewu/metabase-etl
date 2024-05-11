package com.boulderai.metabase.etl.tl.neo4j.config;

import lombok.Data;

import java.util.Date;

/**
 * @ClassName: LogicEntityConfig
 * @Description: 逻辑实体配置
 * @author  df.l
 * @date 2022年10月26日
 * @Copyright boulderaitech.com
 */

@Data
public class LogicEntityConfig {
    private Long  id =0L  ;
    private String name   ;
    private String  code   ;
    private String alias   ;
    private String description ;
    private Integer  level =0  ;
    private Long  parentId  =0L;
    private Long namespaceId  =0L;
    private Integer  status =0  ;
    private String   englishName  ;
    private Boolean   mainEntity=false   ;
    private String  tableName  ;
    private String    dataType  ;
    private String    namespaceCode  ;
}
