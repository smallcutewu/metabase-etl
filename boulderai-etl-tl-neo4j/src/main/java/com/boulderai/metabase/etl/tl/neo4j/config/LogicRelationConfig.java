package com.boulderai.metabase.etl.tl.neo4j.config;

import lombok.Data;

/**
 * @ClassName: LogicRelationConfig
 * @Description: 逻辑实体关系配置
 * @author  df.l
 * @date 2022年10月26日
 * @Copyright boulderaitech.com
 */
@Data
public class LogicRelationConfig {
  private Long id   =0L      ;
    private String   name         ;
    private String   code         ;
    private String    description         ;
    private Long   startId        ;
    private Long   endId       ;
    private Long   startAttrId       ;
    private Long    endAttrId        ;
    private Long    sceneId        ;
    private String   restriction         ;



  private String   startTableName        ;
  private String   endTableName         ;

  private String   startAttrColumnName       ;
  private String   endAttrColumnName         ;
}
