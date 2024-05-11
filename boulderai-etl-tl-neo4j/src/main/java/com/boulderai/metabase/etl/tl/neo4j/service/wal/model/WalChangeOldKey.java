package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;

/**
 * @ClassName: WalChangeOldKey
 * @Description: pg wal更新删除的主键
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public class WalChangeOldKey {

    /**
     *  {
     * 			"keynames": ["id"],
     * 			"keytypes": ["bigint"],
     * 			"keyvalues": [4]
     *  }
     *                主键列名
     */
    @SerializedName("keynames")
    @Expose
    private List<String> keynames;


    /**
     * 主键类型
     */
    @SerializedName("keytypes")
    @Expose
    private List<String> keytypes;


    /**
     * 主键值
     */
    @SerializedName("keyvalues")
    @Expose
    private List<String> keyvalues;

    public List<String> getKeynames() {
        return keynames;
    }

    public void setKeynames(List<String> keynames) {
        this.keynames = keynames;
    }

    public List<String> getKeytypes() {
        return keytypes;
    }

    public void setKeytypes(List<String> keytypes) {
        this.keytypes = keytypes;
    }

    public List<String> getKeyvalues() {
        return keyvalues;
    }

    public void setKeyvalues(List<String> keyvalues) {
        this.keyvalues = keyvalues;
    }

    public   WalChangeOldKey partClone()
    {
        WalChangeOldKey  newWal=new  WalChangeOldKey();
//        newWal.setKeynames(new ArrayList(this.getKeynames()));
//        newWal.setKeytypes(new ArrayList(this.getKeytypes()));
//        newWal.setKeyvalues(new ArrayList(this.getKeyvalues()));
        return newWal;
    }

    @Override
    public String toString() {
        return "WalChangeOldKey{" +
                "keynames=" + keynames +
                ", keytypes=" + keytypes +
                ", keyvalues=" + keyvalues +
                '}';
    }

    public  Column  makeOneColumn(String queryColumn)
    {
        //找列明
        List<String> columnNames=this.getKeynames();

        if(CollectionUtils.isEmpty(columnNames) || !columnNames.contains(queryColumn))
        {
            return  null ;
        }
        //找列类型
        List<String> columnTypes=this.getKeytypes();

        //找列值
        List<String> columnValues=this.getKeyvalues();
//        int size = columnNames.size();
        int size = 1;
        for(int k = 0; k < size; k++)
        {
            String columnName=columnNames.get(k);
            try
            {
                Column column = new Column(columnNames.get(k), columnTypes.get(k), columnValues.get(k));
                return  column;
            }
            catch(Exception ex)
            {
                ex.printStackTrace();
            }

        }

        return null;
    }


    public  Column  makeOneColumn()
    {
        //找列明
        List<String> columnNames=this.getKeynames();
        if(columnNames==null)
        {
            return  null ;
        }
        //找列类型
        List<String> columnTypes=this.getKeytypes();

        //找列值
        List<String> columnValues=this.getKeyvalues();
        int size = columnNames.size();
        for(int k = 0; k < size; k++)
        {
            String columnName=columnNames.get(k);
                try
                {
                    Column column = new Column(columnNames.get(k), columnTypes.get(k), columnValues.get(k));
                    return  column;
                }
                catch(Exception ex)
                {
                    ex.printStackTrace();
                }

        }

        return null;
    }


    /**
     * 将主键列表封装为column对象列表
     * @return  column列表
     */
    public  List<Column>  makePrimaryKeyColumn()
    {
        List<String> columnNames=this.getKeynames();
        if(columnNames==null)
        {
            return  null ;
        }
        List<String> columnTypes=this.getKeytypes();
        List<String> columnValues=this.getKeyvalues();

        /**
         * 检车主键列表是否合法
         */
        Integer[] sizeArray = {columnNames.size(),columnTypes.size(), columnValues.size()};
        Set<Integer> checkValue = new HashSet<Integer>(Arrays.asList(sizeArray));
        if(checkValue.size() > 1 )
        {
            return null;
        }

        /**
         * 组装column对象
         */
        List<Column> columns = new ArrayList<Column>();
        for(int k=0;k<columnNames.size();k++)
        {
            Column column = new Column(columnNames.get(k), columnTypes.get(k), columnValues.get(k));
            columns.add(column);
        }
        return columns;
    }

    public void clear() {
        if (keynames!=null) {
            keynames.clear();
        }
        keynames=null;

        if (keytypes!=null) {
            keytypes.clear();
        }
        keytypes=null;

        if (keyvalues!=null) {
            keyvalues.clear();
        }
        keyvalues=null;
    }
}
