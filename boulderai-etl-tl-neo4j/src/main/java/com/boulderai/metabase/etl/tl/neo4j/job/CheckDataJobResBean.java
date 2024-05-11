package com.boulderai.metabase.etl.tl.neo4j.job;

import lombok.Data;

import java.util.Date;

@Data
public class CheckDataJobResBean {
    private Long id=0L;
    private Long  batchId=0L;
    private String  schemaName;
    private String  tableName;
    private Boolean  dbNetOk;
    private Boolean  neo4jNetOk;
    private Integer pgCount;
    private Integer  neo4jCount;
    private Integer  checkResult;
    private Date  createTime;

}
