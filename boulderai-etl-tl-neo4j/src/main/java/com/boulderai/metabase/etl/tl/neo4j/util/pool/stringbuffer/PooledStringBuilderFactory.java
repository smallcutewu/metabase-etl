package com.boulderai.metabase.etl.tl.neo4j.util.pool.stringbuffer;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectState;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * @ClassName: PooledStringBuilderFactory
 * @Description: StringBuilder实例工厂
 * @author  df.l
 * @date 2022年09月05日
 * @Copyright boulderaitech.com
 */
public class PooledStringBuilderFactory extends BasePooledObjectFactory< StringBuilder> {

    @Override
    public StringBuilder create() throws Exception {
        StringBuilder  bullder=new StringBuilder();
        return bullder;
    }

    @Override
    public PooledObject<StringBuilder> wrap(StringBuilder stringBuilder) {
        return new DefaultPooledObject<StringBuilder>(stringBuilder);
    }

    @Override
    public void destroyObject(PooledObject<StringBuilder> p) throws Exception {
        p.getObject().setLength(0);
    }

    @Override
    public PooledObject<StringBuilder> makeObject() throws Exception {
        return wrap(create());
    }

    @Override
    public boolean validateObject(PooledObject<StringBuilder> p) {
        p.getObject().setLength(0);
        return true;
    }


    @Override
    public void activateObject(PooledObject<StringBuilder> p) throws Exception {
    }


    @Override
    public void passivateObject(PooledObject<StringBuilder> p)
            throws Exception {

    }


}
