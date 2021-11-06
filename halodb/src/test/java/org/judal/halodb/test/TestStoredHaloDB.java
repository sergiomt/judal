package org.judal.halodb.test;

import org.judal.halodb.HaloDBDataSource;
import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.keyvalue.Stored;

import javax.jdo.JDOException;
import java.io.Serializable;

@SuppressWarnings("serial")
public class TestStoredHaloDB implements Stored {

	private final String bucketName;
	private Object key;
	private Serializable value;

	public TestStoredHaloDB(String bucketName) throws JDOException {
		this.bucketName = bucketName;
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
		this.value = value;
	}

	@Override
	public void setContent(byte[] bytes, String contentType) throws JDOException {
		this.value = bytes;
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
	public boolean load(Object key) throws JDOException {
		return load(EngineFactory.getDefaultBucketDataSource(), key);
	}

	@Override
	public boolean load(DataSource dataSource, Object key) throws JDOException {
		return ((HaloDBDataSource) dataSource).openBucket("TestStoredHaloDBBucketName").load(key, this);
	}

	@Override
	public void store() throws JDOException {
		store(EngineFactory.getDefaultBucketDataSource());
	}

	@Override
	public void store(DataSource dataSource) throws JDOException {
		((HaloDBDataSource) dataSource).openBucket("TestStoredHaloDBBucketName").store(this);
	}

	@Override
	public void delete(DataSource dataSource) throws JDOException {
		((HaloDBDataSource) dataSource).openBucket("TestStoredHaloDBBucketName").delete(this.getKey());
	}

	@Override
	public void delete() throws JDOException {
		delete(EngineFactory.getDefaultBucketDataSource());
	}

}
