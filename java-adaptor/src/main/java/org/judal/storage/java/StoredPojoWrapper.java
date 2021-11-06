package org.judal.storage.java;

import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.BucketDataSource;
import org.judal.storage.keyvalue.Stored;

import javax.jdo.JDOException;
import java.io.Serializable;

class StoredPojoWrapper implements Stored {

    private Object key;
    private Stored value;
    private String bucketName;

    public StoredPojoWrapper(Object key) {
        this.key = key;
    }

    private StoredPojoWrapper(Stored stored) {
        this.value = stored;
        this.key = stored.getKey();
        this.bucketName = stored.getBucketName();
    }

    @Override
    public void setKey(Object key) throws JDOException {
        this.key = key;
    }

    @Override
    public Object getKey() throws JDOException {
        return key;
    }

    @Override
    public void setValue(Serializable value) throws JDOException {
        this.value = (Stored) value;
    }

    @Override
    public void setContent(byte[] bytes, String s) throws JDOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getValue() throws JDOException {
        return value;
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    @Override
    public boolean load(Object o) throws JDOException {
        return load(EngineFactory.getDefaultBucketDataSource(), o);
    }

    @Override
    public boolean load(DataSource dataSource, Object o) throws JDOException {
        boolean loaded = false;
        value = (Stored) o;
        key = ((Stored) o).getKey();
        try (Bucket bck = ((BucketDataSource) dataSource).openBucket(value.getBucketName())) {
            loaded = bck.load(key, value);
        }
        return loaded;
    }

    @Override
    public void store() throws JDOException {
        store(EngineFactory.getDefaultBucketDataSource());
    }

    @Override
    public void store(DataSource dataSource) throws JDOException {
        try (Bucket bck = ((BucketDataSource) dataSource).openBucket(value.getBucketName())) {
            bck.store(value);
        }
    }

    @Override
    public void delete(DataSource dataSource) throws JDOException {
        try (Bucket bck = ((BucketDataSource) dataSource).openBucket(value.getBucketName())) {
            bck.delete(key);
        }
    }

    @Override
    public void delete() throws JDOException {
        delete(EngineFactory.getDefaultBucketDataSource());
    }

    public static StoredPojoWrapper wrap(Stored stored) {
        return new StoredPojoWrapper(stored);
    }

    public static Stored unwrap (StoredPojoWrapper wrapper) {
        return (Stored) wrapper.getValue();
    }

}

