package com.boulderai.metabase.etl.tl.neo4j.service.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName: PipelineContext
 * @Description: pipeline上下文环境
 * @author  df.l
 * @date 2022年08月09日
 * @Copyright boulderaitech.com
 */
public class PipelineContext {
    private static final Map<String,String> CHILD_PARENT_TABLE_MAPPING = new ConcurrentHashMap<String,String>(16);
    private static final Map<String, List<String>> TABLE_COLUMN_MAP = new ConcurrentHashMap<String,List<String>>(16);


    public static  Boolean hasParentTableColumn(String tableName,String parentColumn)
    {
        List<String>  columnList= TABLE_COLUMN_MAP.get(tableName);
        if(columnList==null)
        {
            return false;
        }
        return columnList.contains(parentColumn);
    }


    public  static  void addParentTableColumn(String tableName,String parentColumn)
    {
        List<String>  columnList= TABLE_COLUMN_MAP.get(tableName);
        if(columnList==null)
        {
            columnList=new ArrayList<String>(8);
            TABLE_COLUMN_MAP.put(tableName,columnList);
        }
        if(!columnList.contains(parentColumn))
        {
            columnList.add(parentColumn);
        }

    }


    public  static  Boolean  hasInheritTable(String  tableName)
    {
        return  CHILD_PARENT_TABLE_MAPPING.containsKey(tableName);
    }


    public static String getInheritTable(String  tableName)
    {
        return  CHILD_PARENT_TABLE_MAPPING.get(tableName);
    }

  
    public  static  void addAllMapping(Map<String,String>  mapping)
    {
        CHILD_PARENT_TABLE_MAPPING.putAll(mapping);
    }

    public static void addTableColumns(String  tableName, List<String> columnList)
    {
        TABLE_COLUMN_MAP.put(tableName, columnList);
    }

 
    public static  List<String> getTableColumns(String  tableName)
    {
        return   TABLE_COLUMN_MAP.get(tableName);
    }


    public  static void addInheritTableMapping(String childName,String parentName)
    {
        CHILD_PARENT_TABLE_MAPPING.put(childName, parentName);
    }

    public  static String getParentTableByChild(String childName)
    {
       return  CHILD_PARENT_TABLE_MAPPING.get(childName);
    }

    public static void  clear()
    {
        CHILD_PARENT_TABLE_MAPPING.clear();
        TABLE_COLUMN_MAP.clear();
    }
}
