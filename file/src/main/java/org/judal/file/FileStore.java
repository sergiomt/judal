package org.judal.file;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import javax.jdo.JDOException;

import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.keyvalue.BucketDataSource;
import org.judal.storage.keyvalue.Stored;

import com.knowgate.io.FileUtils;

public class FileStore implements Stored {

	private static final long serialVersionUID = 1L;

	private String bucketName;
	private String fileName;
	private byte[] content;

	public FileStore(FileBucket bucket, String name) {
		fileName = name;
		bucketName = bucket.name();
	}
	
	@Override
	public void setKey(Object key) throws JDOException {
		fileName = (String) key;
	}

	@Override
	public Object getKey() throws JDOException {
		return fileName;
	}

	@Override
	public void setValue(Serializable value) throws JDOException {
	}

	@Override
	public void setContent(byte[] bytes, String contentType) throws JDOException {
		content = bytes;
	}

	@Override
	public byte[] getValue() throws JDOException {
		return content;
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
		FileDataSource fdts = (FileDataSource) dataSource;
		boolean retval;
		try (FileBucket bucket = fdts.openBucket(bucketName)) {
			retval = bucket.load(key, this);
		}
		return retval;
	}

	@Override
	public void store() throws JDOException {
		store(EngineFactory.getDefaultBucketDataSource());
	}

	@Override
	public void store(DataSource dataSource) throws JDOException {
		FileDataSource fdts = (FileDataSource) dataSource;
		try (FileBucket bucket = fdts.openBucket(bucketName)) {
			bucket.store(this);
		}
	}

	@Override
	public void delete(DataSource dataSource) throws JDOException {
		FileDataSource fdts = (FileDataSource) dataSource;
		try (FileBucket bucket = fdts.openBucket(bucketName)) {
			bucket.delete(getKey());
		}
	}

	@Override
	public void delete() throws JDOException {
		delete(EngineFactory.getDefaultBucketDataSource());
	}

	
}
