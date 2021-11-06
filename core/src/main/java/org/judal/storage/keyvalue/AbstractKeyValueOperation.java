package org.judal.storage.keyvalue;

import javax.jdo.JDOException;

import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.Operation;

public abstract class AbstractKeyValueOperation<S extends Stored> implements Operation {

    protected Bucket bckt;
    protected BucketDataSource dts;
    private S stored;

    /**
     * <p>Constructor.</p>
     * Create KeyValueOperation using EngineFactory default bucket data source.
     */
    protected AbstractKeyValueOperation() throws NullPointerException {
        this(EngineFactory.getDefaultBucketDataSource());
    }

    /**
     * <p>Constructor.</p>
     * Create KeyValueOperation using given table data source.
     * @param dataSource BucketDataSource
     * @throws NullPointerException if dataSource is null
     */
    protected AbstractKeyValueOperation(BucketDataSource dataSource) throws NullPointerException {
        if (null==dataSource)
            throw new NullPointerException("AbstractKeyValueOperation constructor. BucketDataSource cannot be null");
        dts = dataSource;
        stored = null;
    }

    /**
     * <p>Constructor.</p>
     * Create KeyValueOperation using given table data source.
     * @param dataSource BucketDataSource
     * @param record R Instance of Stored subclass to be used by this operation.
     * @throws NullPointerException if dataSource is null or record is null
     */
    protected AbstractKeyValueOperation(BucketDataSource dataSource, S record) throws NullPointerException {
        if (null==dataSource)
            throw new NullPointerException("AbstractKeyValueOperation constructor. BucketDataSource cannot be null");
        if (null==record)
            throw new NullPointerException("BucketDataSource constructor. Record cannot be null");
        dts = dataSource;
        stored = record;
        open();
    }

    @Override
    public boolean exists(Object key) {
        return getBucket().exists(key);
    }

    @Override
    public void delete(Object key) throws JDOException {
        getBucket().delete(key);
    }

    @Override
    public DataSource dataSource() throws JDOException {
        return dts;
    }

    @Override
    public void close() throws Exception {
        if (null!=bckt) {
            bckt.close();
            bckt = null;
        }
    }

    protected Bucket getBucket() {
        return bckt;
    }

    protected Stored getStored() {
        return stored;
    }

    protected void open() {
        bckt = ((BucketDataSource) dts).openBucket(stored.getBucketName());
    }
}
