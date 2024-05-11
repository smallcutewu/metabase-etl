package com.boulderai.metabase.etl.tl.neo4j.config;

import lombok.Data;
import lombok.ToString;

import java.util.Date;

/**
 * @ClassName: ColumnToFieldConfig
 * @Description: 从列到类field定义配置
 * @author  df.l
 * @date 2022年10月12日
 * @Copyright boulderaitech.com
 */
@Data
@ToString
public class ColumnToFieldConfig {
    
//      private Long        id  ;
      private String       tableColumn ;
      private String         beanField ;
      private String         beanName ;
      private String        description ;

      private Boolean    primaryKey=false;

      private  String defaultValue;

      private String  beanFieldType;

      private String extendColumnSql;
      private String extendSqlColumns;
      private String extendSqlReturnField;

}
