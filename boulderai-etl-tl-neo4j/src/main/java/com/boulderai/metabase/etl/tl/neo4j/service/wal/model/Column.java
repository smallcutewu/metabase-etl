package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;


import com.boulderai.metabase.lang.Constants;
import com.boulderai.metabase.etl.tl.neo4j.util.PostgresqlWalUtil;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.sql.Time;
import java.util.Date;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @ClassName: Column
 * @Description: pg wal列映射对象
 * @author  df.l
 * @date 2022年10月11日
 * @Copyright boulderaitech.com
 */

public class Column {

    private static  final Pattern timePatternOne=Pattern.compile("\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}");
    private static  final  Pattern   timePatternTwo=Pattern.compile("\\d{4}-\\d{1,2}-\\d{1,2}");
    private static  final  Pattern    timePatternThree=Pattern.compile("\\d{11,15}");
    private static  final Pattern timePatternForu=Pattern.compile("\\d{1,2}:\\d{2}:\\d{2}");

    private static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";

    private String name;
    private String type;
    private String value;

    private Object cValue=null;

    public Object  getRealValue()
    {
        if(cValue==null)
        {
            return null;
        }
        if(cValue.getClass() == Date.class)
        {
            Date dt= (Date) cValue;
           return new DateTime(dt).toString(DEFAULT_DATETIME_FORMAT);
        }
        else  if(cValue.getClass() == Time.class)
        {
            Time dt= (Time) cValue;
            return new DateTime(dt).toString(DEFAULT_TIME_FORMAT);
        }
        return  cValue;
    }

    public Column(String name, String type, String value) {
        this.name = name;
        this.type = type;
        this.value = value;
        caclType();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Column column = (Column) o;
        return name.equals(column.name) && type.equals(column.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    private void caclType()
    {
        if(  this.type.contains("char")||
                this.type.contains("character")
        || this.type.contains("text")
                ||this.type.contains("hstore")

        )
        {
            this.cValue=value;
        }
        else if(this.type.contains("numeric")   )
        {
            this.cValue=NumberUtils.toDouble(value,0f);
        }
        else if(this.type.contains("int8") ||this.type.contains("bigint")   )
        {
            this. cValue= NumberUtils.toLong(value,0);
        }
        else if(this.type.contains("int4") ||this.type.contains("int2")
                ||this.type.contains("smallint")   )
        {
            this.cValue=  NumberUtils.toInt(value,0);
        }
        else if(this.type.contains("boolean"))
        {
            this.cValue= BooleanUtils.toBoolean(value);
        }
        else if(this.type.contains("float8")
             ||this.type.contains("money")
         )
        {
            this.cValue=NumberUtils.toDouble(value,0D);
        }
        else if(this.type.contains("float4"))
        {
            this.cValue=NumberUtils.toFloat(value,0f);
        }
        else if(this.type.contains("timestamp")|| this.type.contains("date")  )
        {
            this.cValue=this.parseDate(value);
        }
        else if(this.type.contains("time")  )
        {
            Date date= parseDate(value);
            java.sql.Time time = new java.sql.Time(date.getTime());
            this.cValue=time;
        }
        else if(this.type.contains("bit"))
        {
            this.cValue= NumberUtils.toInt(value,0)>0?true:false;
        }
        else
        {
            this.cValue=value;
        }



    }


//
//    protected Object resolveValue(String typeName, String columnValue) {
//        value.setValue(columnValue);
//
//        if (value.isNull()) {
//            // nulls are null
//            return null;
//        }
//
//        switch (typeName) {
//            case "boolean":
//            case "bool":
//                return value.asBoolean();
//
//            case "integer":
//            case "int":
//            case "int4":
//            case "smallint":
//            case "int2":
//            case "smallserial":
//            case "serial":
//            case "serial2":
//            case "serial4":
//                return value.asInteger();
//
//            case "bigint":
//            case "bigserial":
//            case "int8":
//            case "oid":
//                return value.asLong();
//
//            case "real":
//            case "float4":
//                return value.asFloat();
//
//            case "double precision":
//            case "float8":
//                return value.asDouble();
//
//            case "numeric":
//            case "decimal":
//                return value.asBigDecimal();
//
//            case "character":
//            case "char":
//            case "character varying":
//            case "varchar":
//            case "bpchar":
//            case "text":
//            case "hstore":
//                return value.asString();
//
//            case "date":
//                return value.asDate();
//
//            case "timestamp with time zone":
//            case "timestamptz":
//                return value.asOffsetDateTimeAtUtc();
//
//            case "timestamp":
//            case "timestamp without time zone":
//                return value.asTimestamp();
//
//            case "time":
//                return value.asTime();
//
//            case "time without time zone":
//                return value.asLocalTime();
//
//            case "time with time zone":
//            case "timetz":
//                return value.asOffsetTimeUtc();
//
//            case "bytea":
//                return value.asByteArray();
//
//            case "box":
//                return value.asBox();
//            case "circle":
//                return value.asCircle();
//            case "interval":
//                return value.asInterval();
//            case "line":
//                return value.asLine();
//            case "lseg":
//                return value.asLseg();
//            case "money":
//                final Object v = value.asMoney();
//                return (v instanceof PGmoney) ? ((PGmoney) v).val : v;
//            case "path":
//                return value.asPath();
//            case "point":
//                return value.asPoint();
//            case "polygon":
//                return value.asPolygon();
//
//            case "geometry":
//            case "geography":
//            case "citext":
//            case "bit":
//            case "bit varying":
//            case "varbit":
//            case "json":
//            case "jsonb":
//            case "xml":
//            case "uuid":
//            case "tsrange":
//            case "tstzrange":
//            case "daterange":
//            case "inet":
//            case "cidr":
//            case "macaddr":
//            case "macaddr8":
//            case "int4range":
//            case "numrange":
//            case "int8range":
//                return value.asString();
//
//            case "pg_lsn":
//            case "tsquery":
//            case "tsvector":
//            case "txid_snapshot":
//            default:
//                return null;
//        }
//
//    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        if (this.type.equals("money")){
            return PostgresqlWalUtil.extractNumeric(this.value);
        }else{
            return value;
        }
    }

    public void setValue(String value) {
        this.value = value;

    }


    public    Date parseDate(String value)
    {
        if(StringUtils.isNotBlank(value))
        {
            value=value.trim();
        }
        else
        {
            return null;
        }
        Date date=null;
        Matcher m= timePatternOne.matcher(value);
        if(m.find())
        {
            int maxLength=Constants.DATE_LONG_FORMAT.length();
            if(value.length()>maxLength)
            {
                value=value.substring(0,maxLength);
            }
            date=    DateTime.parse(value, DateTimeFormat.forPattern(Constants.DATE_LONG_FORMAT)).toDate();
//            date=value;
        }
        else
        {
            m = timePatternThree.matcher(value);
            if(m.find())
            {
                if (value.length()>=13)
                {
                    value = value.substring(0,13);
                }
                Long timeNum= org.apache.commons.lang3.math.NumberUtils.toLong(value,0L);
                date=  new   DateTime(timeNum).toDate();
            }
            else
            {
                m = timePatternTwo.matcher(value);
                if(m.find())
                {
                    date=    DateTime.parse(value, DateTimeFormat.forPattern(Constants.DATE_SHORT_FORMAT)).toDate();
                }
                else {
                    m = timePatternForu.matcher(value);
                    if(m.find())
                    {
                        date=    DateTime.parse(value, DateTimeFormat.forPattern(Constants.TIME_SHORT_FORMAT)).toDate();
                    }
                }


            }
        }

        return date;
    }

    public  void clear()
    {
        this.name=null;
        this.type=null;
        this.value=null;
        this. cValue=null;
    }
}
