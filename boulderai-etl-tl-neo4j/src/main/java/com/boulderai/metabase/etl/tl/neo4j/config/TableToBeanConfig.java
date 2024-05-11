package com.boulderai.metabase.etl.tl.neo4j.config;

import lombok.Data;
import lombok.ToString;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * @ClassName: TableToBeanConfig
 * @Description:是否持久化表和关系配置
 * @author  df.l
 * @date 2022年10月12日
 * @Copyright boulderaitech.com
 */
@Data
@ToString
public class TableToBeanConfig {
     private Long    id;
    private String   tableName;
    private String   className;
    private String    description;
    private List<ColumnToFieldConfig> columnToFieldConfigList;
    private List<ColumnToFieldConfig> notKeyColumnToFieldConfigList;

    private String checkColumn;
    private String checkValue;
    private String    beanName ;
    private List<Object> checkValueList;

    private String  synType;

    private String  relationName;


    private String deleteColumn;
    private String deleteColumnValue ;

    private Map<String,String>  column2FieldMap=new HashMap<String, String>();
    private Map<String,String>  field2ColumnMap=new HashMap<String, String>();

    private  ColumnToFieldConfig     primaryKeyFieldConfig ;


    public Boolean hasCheckValue(Object  value)
    {
       return  CollectionUtils.isNotEmpty(checkValueList)&&checkValueList.contains(value);
    }


    public  void addColumnToFieldConfig(ColumnToFieldConfig  config)
    {
        if(columnToFieldConfigList==null)
        {
            columnToFieldConfigList= new ArrayList<ColumnToFieldConfig>();
        }
        columnToFieldConfigList.add(config);
    }

    public  void init()
    {
        for (ColumnToFieldConfig config : columnToFieldConfigList) {
             String       tableColumn =config.getTableColumn();
             String        beanField =config.getBeanField();
             column2FieldMap.put(tableColumn,beanField);
             field2ColumnMap.put(beanField,tableColumn);

             if(config.getPrimaryKey())
             {
                 primaryKeyFieldConfig=config;
             }
             else {
                 if(notKeyColumnToFieldConfigList==null)
                 {
                     notKeyColumnToFieldConfigList= new ArrayList<ColumnToFieldConfig>();
                 }
                 notKeyColumnToFieldConfigList.add(config);
             }
        }

        if(StringUtils.isNotBlank(checkValue))
        {
            String[]  realCheckValue=checkValue.trim().split(",");
            if(realCheckValue.length>0)
            {
                checkValueList=Arrays.asList(realCheckValue);
            }
        }

        if(primaryKeyFieldConfig==null)
        {
            throw  new RuntimeException("da_column_to_field_config  must config primary key! tableName:"+tableName);
        }
    }

    public  String  getColumnByField(String fieldName)
    {
        return field2ColumnMap.get(fieldName);
    }

    public  String  getFieldByColumn(String columnName)
    {
        return column2FieldMap.get(columnName);
    }

}

