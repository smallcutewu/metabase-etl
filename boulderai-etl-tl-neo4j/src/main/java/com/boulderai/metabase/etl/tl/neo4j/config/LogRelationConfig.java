package com.boulderai.metabase.etl.tl.neo4j.config;

import lombok.Data;
import lombok.ToString;

@Data
@ToString(callSuper = true)
public class LogRelationConfig {
    private String  sourceTableName;
    private Long    sourceTableId;

    private Long      id ;
    private String    name;
    private String    code;
    private Long      startId ;
    private String    startTableName ;

    private Long      endId ;
    private String    endTableName ;

    private Long      startAttrId;
    private String    startAttrName ;

    private Long      endAttrId ;
    private String    endAttrName ;


    private Long       sceneId=0L;
    private String      restriction;
    private Long       namespaceId ;
    private Integer      status ;
    private Integer       type;
    private String       labelId ;
    private Integer        isRer ;
    private String       attrMapping ;
}
