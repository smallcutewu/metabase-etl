package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ColumnTest {

    @Test
    public void  testColumnValueTex(){
        Column  column =new Column("id","text","china");
        assertNotNull(column.getValue());
        assertEquals("china",column.getRealValue());
    }

    @Test
    public void  testColumnValueCharacter(){
        Column  column =new Column("id","character","china");
        assertNotNull(column.getValue());
        assertEquals("china",column.getRealValue());
    }

    @Test
    public void  testColumnValueChar(){
        Column  column =new Column("id","char","china");
        assertNotNull(column.getValue());
        assertEquals("china",column.getRealValue());
    }


    @Test
    public void  testColumnValueNumeric(){
        Column  column =new Column("id","numeric","100");
        assertNotNull(column.getValue());
        assertEquals(100D,column.getRealValue());
    }

    @Test
    public void  testColumnValueBigint(){
        Column  column =new Column("id","bigint","99");
        assertNotNull(column.getValue());
        assertEquals(99L,column.getRealValue());
    }

    @Test
    public void  testColumnValueInt8(){
        Column  column =new Column("id","int8","99");
        assertNotNull(column.getValue());
        assertEquals(99L,column.getRealValue());
    }

    @Test
    public void  testColumnValueInt4(){
        Column  column =new Column("id","int4","98");
        assertNotNull(column.getValue());
        assertEquals(98,column.getRealValue());
    }

    @Test
    public void  testColumnValueInt2(){
        Column  column =new Column("id","int2","98");
        assertNotNull(column.getValue());
        assertEquals(98,column.getRealValue());
    }

    @Test
    public void  testColumnValueSmallint(){
        Column  column =new Column("id","smallint","98");
        assertNotNull(column.getValue());
        assertEquals(98,column.getRealValue());
    }



    @Test
    public void  testColumnValueBoolean(){
        Column  column =new Column("id","boolean","true");
        assertNotNull(column.getValue());
        assertEquals(true,column.getRealValue());
    }


    @Test
    public void  testColumnValueFloat8(){
        Column  column =new Column("id","float8","99.8");
        assertNotNull(column.getValue());
        assertEquals(99.8d,column.getRealValue());
    }

    @Test
    public void  testColumnValueMoney(){
        Column  column =new Column("id","money","99.8");
        assertNotNull(column.getValue());
        assertEquals(99.8d,column.getRealValue());
    }

    @Test
    public void  testColumnValueFloat4(){
        Column  column =new Column("id","float4","99.8");
        assertNotNull(column.getValue());
        assertEquals(99.8f,column.getRealValue());
    }

    @Test
    public void  testColumnValueBit(){
        Column  column =new Column("id","bit","1");
        assertNotNull(column.getValue());
        assertEquals(true,column.getRealValue());
    }

    @Test
    public void  testColumnValueOther(){
        Column  column =new Column("id","other","fck");
        assertNotNull(column.getValue());
        assertEquals("fck",column.getRealValue());
    }

    @Test
    public void  testColumnValueTime(){
        String vTime="18:20:54";
        Column  column =new Column("id","time","18:20:54");
        assertNotNull(column.getValue());
        assertNotNull(column.getRealValue());
    }


    @Test
    public void  testColumnValueTimeLongFormat(){
        String vTime="2021-12-22 20:32:54";
        Column  column =new Column("id","timestamp",vTime);
        assertNotNull(column.getValue());
        assertNotNull(column.getRealValue());
    }

    @Test
    public void  testColumnValueTimeNumber(){
        String vTime="1660809252000";
        Column  column =new Column("id","timestamp",vTime);
        assertNotNull(column.getValue());
        assertNotNull(column.getRealValue());
    }

    @Test
    public void  testColumnValueShortDate(){
        String vTime="2021-12-22";
        Column  column =new Column("id","timestamp",vTime);
        assertNotNull(column.getValue());
        assertNotNull(column.getRealValue());
    }

}
