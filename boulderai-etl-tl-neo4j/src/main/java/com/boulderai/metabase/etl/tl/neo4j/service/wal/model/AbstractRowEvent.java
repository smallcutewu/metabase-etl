package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;


import java.util.List;

/**
 * @ClassName: AbstractWalEvent
 * @Description: pg wal 行事件基础组装模型
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */


public class AbstractRowEvent extends AbstractWalEvent {
    private String schemaName;
    private String tableName;
    private List<Column> primaryKeyColumns;
    private List<Column> columns;

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public EventType getEventType() {
        return null;
    }

    public void setPrimaryKeyColumns(List<Column> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public List<Column> getPrimaryKeyColumns(){
        return this.primaryKeyColumns;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }


    public Column getColumn(String name){
        return this.columns.stream().filter(column -> column.getName().equals(name)).findFirst().orElseGet(null);
    }


    @Override
    public void clear()
    {
        this.schemaName=null;
        this.tableName=null;
        if( primaryKeyColumns!=null)
        {
            primaryKeyColumns.clear();
            primaryKeyColumns=null;
        }

        if( columns!=null)
        {
            columns.clear();
            columns=null;
        }

    }
}