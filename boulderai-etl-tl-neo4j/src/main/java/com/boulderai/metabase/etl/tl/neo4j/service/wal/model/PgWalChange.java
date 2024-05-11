package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;


/**
 * @ClassName: PgWalChange
 * @Description: pg wal日志记录反序列化后的对象
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public class PgWalChange {

    private  final static Logger logger = LoggerFactory.getLogger(PgWalChange.class);

    private String changeId;

    private String  parentTable;

    /**
     * 数据变化类型
     */
    @SerializedName("kind")
    @Expose
    private String kind;

    /**
     * 数据库的schema
     */
    @SerializedName("schema")
    @Expose
    private String schema;


    /**
     * 数据表
     */
    @SerializedName("table")
    @Expose
    private String table;


    /**
     * 列明列表
     */
    @SerializedName("columnnames")
    @Expose
    private List<String> columnnames = null;


    /**
     * 列类型；列表
     */
    @SerializedName("columntypes")
    @Expose
    private List<String> columntypes = null;

    /**
     * 列值列表
     */
    @SerializedName("columnvalues")
    @Expose
    private List<String> columnvalues = null;

    /**
     * 更新删除的主键key
     */
    @SerializedName("oldkeys")
    @Expose
    private   WalChangeOldKey   oldkeys;

    private Map<Object,Object> before;

    private Map<Object,Object> after;

    private  List<String>  relationCqlList;

    private   String rData;

//    public List<String> getRelationCqlList() {
//        return relationCqlList;
//    }
//
//    public void setRelationCqlList(List<String> relationCqlList) {
//        this.relationCqlList = relationCqlList;
//    }

    /**
     * 将walchange组装为eventrow
     */
    private  AbstractRowEvent  changeToRow;

    public AbstractRowEvent getChangeToRow() {
        return changeToRow;
    }

    public WalChangeOldKey getOldkeys() {
        return oldkeys;
    }

    public Boolean checkChangeIsOK()
    {
        return     columnnames != null || oldkeys!=null ;
    }

    public   PgWalChange partClone()
    {
        PgWalChange  newWal=new  PgWalChange();
        newWal.setKind(this.getKind());
        newWal.setTable(this.getTable());
        newWal.setChangeId(this.getChangeId());
        newWal.setSchema(this.getSchema());
//        newWal.setColumnnames(new ArrayList(this.getColumnnames()));
//        newWal.setColumntypes(new ArrayList(this.getColumntypes()));
//        newWal.setColumnvalues(new ArrayList(this.getColumnvalues()));
        if(oldkeys!=null)
        {
            newWal.setOldkeys(oldkeys.partClone());
        }
        return newWal;
    }

    public void setChangeToRow(AbstractRowEvent changeToRow) {
        this.changeToRow = changeToRow;
    }

    public void setOldkeys(WalChangeOldKey oldkeys) {
        this.oldkeys = oldkeys;
    }

    public String getChangeId() {
        return changeId;
    }

    public void setChangeId(String changeId) {
        this.changeId = changeId;
    }

    public String getParentTable() {
        return parentTable;
    }

    public void setParentTable(String parentTable) {
        this.parentTable = parentTable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PgWalChange that = (PgWalChange) o;
        return changeId.equals(that.changeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(changeId);
    }

    public PgWalChange() {
    }


    public PgWalChange(
            String kind,
            String schema,
            String table,
            List<String> columnnames,
            List<String> columntypes,
            List<String> columnvalues
    ) {
        super();
        this.kind = kind;
        this.schema = schema;
        this.table = table;
        this.columnnames = columnnames;
        this.columntypes = columntypes;
        this.columnvalues = columnvalues;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getColumnnames() {
        return columnnames;
    }

    public void setColumnnames(List<String> columnnames) {
        this.columnnames = columnnames;
    }

    public List<String> getColumntypes() {
        return columntypes;
    }

    public void setColumntypes(List<String> columntypes) {
        this.columntypes = columntypes;
    }

    public List<String> getColumnvalues() {
        return columnvalues;
    }

    public void setColumnvalues(List<String> columnvalues) {
        this.columnvalues = columnvalues;
    }

    public Map<Object, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<Object, Object> before) {
        this.before = before;
    }

    public Map<Object, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<Object, Object> after) {
        this.after = after;
    }

    /**
     * 清除属性值
     */
    public void clear()
    {
        this.parentTable=null;
        if(columnnames!=null)
        {
            columnnames.clear();
            columnnames=null;
        }

        if(columntypes!=null)
        {
            columntypes.clear();
            columntypes=null;
        }


        if(columnvalues!=null)
        {
            columnvalues.clear();
            columnvalues=null;
        }
        if(changeToRow!=null)
        {
            changeToRow.clear();
            this.changeToRow =null;
        }

        this.kind=null;
        this.schema=null;
        this.table=null;

        if(relationCqlList!=null)
        {
            relationCqlList.clear();
        }
        relationCqlList=null;

        if (oldkeys!=null) {
            oldkeys.clear();
        }
        oldkeys=null;
        this.rData=null;

    }

    @Override
    public String toString() {
        return "PgWalChange{" +
                "changeId='" + changeId + '\'' +
                ", kind='" + kind + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", columnnames=" + columnnames +
                ", columntypes=" + columntypes +
                ", columnvalues=" + columnvalues +
                '}';
    }

    public  Column  makeOneColumn(String queryColumnName)
    {
        //找列明
        List<String> columnNames=this.getColumnnames();
        if(CollectionUtils.isEmpty(columnNames) || !columnNames.contains(queryColumnName) )
        {
            if(this.getOldkeys()!=null)
            {
                return   this.getOldkeys().makeOneColumn(queryColumnName);
            }
            return  null ;
        }

        //找列类型
        List<String> columnTypes=this.getColumntypes();

        //找列值
        List<String> columnValues=this.getColumnvalues();
        int size = columnNames.size();
        for(int k = 0; k < size; k++)
        {
            String columnName=columnNames.get(k);
            if(columnName.equals(queryColumnName))
            {
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
        }

        return null;
    }

    /**
     * 将列明、列类型、类值组装成   Column对象
     * @return
     */
    public  List<Column>  makeColumn()
    {
        //找列明
        List<String> columnNames=this.getColumnnames();
        if(columnNames==null)
        {
            return  null ;
        }

        //找列类型
        List<String> columnTypes=this.getColumntypes();

        //找列值
        List<String> columnValues=this.getColumnvalues();

        Integer[] sizeArray = {columnNames.size(),columnTypes.size(), columnValues.size()};

        //判断列是否合法
        Set<Integer> checkValue = new HashSet<Integer>(Arrays.asList(sizeArray));
        if(checkValue.size() > 1 )
        {
            logger.error("key and value and type have to be same length! Detail: "+this);
            return null;
        }

        //组装成Column
        List<Column> columns = new ArrayList<Column>();
        for(int k=0;k<columnNames.size();k++)
        {
            try
            {
                Column column = new Column(columnNames.get(k), columnTypes.get(k), columnValues.get(k));
                columns.add(column);
            }
            catch(Exception ex)
            {
                ex.printStackTrace();
            }

        }
        return columns;
    }

    public String   getTableCurrentId()
    {
        StringBuilder  sb=new StringBuilder("");
        if(kind.equals(EventType.INSERT.toString())
                 || kind.equals(EventType.UPDATE.toString())
           )
        {
           int size=columnnames.size();
           for (int i = 0; i < size; i++) {
               String columnName= columnnames.get(i);
               String columnType= columntypes.get(i);
               sb.append(columnName).append("=").append(columnType);
           }
        }
        else
        {
            int size=oldkeys.getKeynames().size();
            for (int i = 0; i < size; i++) {
                String columnName= oldkeys.getKeynames().get(i);
                String columnType= oldkeys.getKeytypes().get(i);
                sb.append(columnName).append("=").append(columnType);
            }
        }
        return  DigestUtils.md5Hex(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    public String getrData() {
        return rData;
    }

    public void setrData(String rData) {
        this.rData = rData;
    }
}
