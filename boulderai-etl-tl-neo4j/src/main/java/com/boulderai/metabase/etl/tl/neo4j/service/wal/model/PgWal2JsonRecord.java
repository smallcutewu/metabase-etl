package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;


import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * @ClassName: PgWal2JsonRecord
 * @Description: pg wal日志反序列化后的对象
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */
public class PgWal2JsonRecord {

    /**
     * 解析  wal json：
     * Insert json：
     *
     * {
     * 	"xid": 12583810,
     * 	"nextlsn": "1B/15C9BFD0",
     * 	"change": [{
     * 		"kind": "insert",
     * 		"schema": "public",
     * 		"table": "pg_wal_test",
     * 		"columnnames": ["id", "name", "price", "closed", "quality", "test"],
     * 		"columntypes": ["bigint", "character varying(255)", "numeric(10,2)", "boolean", "bigint", "smallint"],
     * 		"columnvalues": [5, "ggg", 56.80, false, 21, 5]
     *        }]
     * }
     *
     * Update  json：
     * {
     * 	"xid": 12583798,
     * 	"nextlsn": "1B/15C97398",
     * 	"change": [{
     * 		"kind": "update",
     * 		"schema": "public",
     * 		"table": "pg_wal_test",
     * 		"columnnames": ["id", "name", "price", "closed", "quality", "test"],
     * 		"columntypes": ["bigint", "character varying(255)", "numeric(10,2)", "boolean", "bigint", "smallint"],
     * 		"columnvalues": [3, "bbb", 11.00, false, 11, 1],
     * 		"oldkeys": {
     * 			"keynames": ["id"],
     * 			"keytypes": ["bigint"],
     * 			"keyvalues": [3]
     *                }* 	}]
     * }
     *
     * Delete  json：
     * {
     * 	"xid": 12583814,
     * 	"nextlsn": "1B/15C9EF08",
     * 	"change": [{
     * 		"kind": "delete",
     * 		"schema": "public",
     * 		"table": "pg_wal_test",
     * 		"oldkeys": {
     * 			"keynames": ["id"],
     * 			"keytypes": ["bigint"],
     * 			"keyvalues": [4]
     *                }* 	}]
     * }
     */


    @SerializedName("xid")
    @Expose
    private long xid=0L;

    @SerializedName("change")
    @Expose
    private List<PgWalChange> change = null;


    @SerializedName("nextlsn")
    @Expose
    private   String   nextlsn  =null;


    public PgWal2JsonRecord() {
    }

    public PgWal2JsonRecord(long xid, List<PgWalChange> change,String   nextlsn) {
        super();
        this.xid = xid;
        this.change = change;
        this.nextlsn=nextlsn;
    }


    public PgWal2JsonRecord(long xid, List<PgWalChange> change) {
        super();
        this.xid = xid;
        this.change = change;
    }

    public long getXid() {
        return xid;
    }

    public void setXid(long xid) {
        this.xid = xid;
    }

    public List<PgWalChange> getChange() {
        return change;
    }

    public void setChange(List<PgWalChange> change) {
        this.change = change;
    }


    public String getNextlsn() {
        return nextlsn;
    }

    public void setNextlsn(String nextlsn) {
        this.nextlsn = nextlsn;
    }

    public void clear()
    {
        if(change!=null)
        {
            change.clear();
            change=null;
        }
        nextlsn  =null ;
    }

    /**
     * 移除引用，不是删除change列表
     */
    public void unLinkPgWalChange()
    {
        change.clear();
        change=null;
    }

    @Override
    public String toString() {
        return "PgWal2JsonRecord{" +
                "xid=" + xid +
                ", change=" + change +
                ", nextlsn='" + nextlsn + '\'' +
                '}';
    }
}
