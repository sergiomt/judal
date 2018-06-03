package org.judal.file;

import java.io.File;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;

import javax.transaction.TransactionManager;

import org.judal.storage.keyvalue.BucketDataSource;
import org.judal.file.InMemorySequence;
import org.judal.storage.Param;

public class FileDataSource implements BucketDataSource {

	private ConcurrentHashMap<String, ConcurrentHashMap<String,FileBucket>> buckets;
	private ConcurrentHashMap<String, Sequence> sequences;

	private Map<String, String> properties;

	public FileDataSource(Map<String, String> properties) {
		this.buckets = new ConcurrentHashMap<>();
		this.sequences = new ConcurrentHashMap<>();
		this.properties = properties;
	}

	@Override
	public boolean exists(String objectName, String objectType) throws JDOException {
		if ("U".equals(objectType)) {
			if (buckets.containsKey(objectName.toLowerCase())) {
				return true;
			} else {
				String path = properties.get(URI);
				if (!path.endsWith(File.separator))
					path += File.separator;
				File dir = new File(path+objectName);
				return dir.exists() && dir.isDirectory();
			}
		} else {
			return false;
		}
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public TransactionManager getTransactionManager() {
		throw new JDOUnsupportedOptionException("FileDataSource does not support transactions");
	}

	@Override
	public JDOConnection getJdoConnection() throws JDOException {
		throw new JDOUnsupportedOptionException("FileDataSource does not support connections");
	}

	public InMemorySequence createSequence(String name, long initial) throws JDOException {
		if (sequences.containsKey(name.toLowerCase()))
			throw new JDOException("Sequence " + name + " already exists");
		InMemorySequence seq = new InMemorySequence(name, initial);
		sequences.put(name.toLowerCase(), seq);
		return seq;
	}

	public void dropSequence(String name, long initial) throws JDOException {
		if (!sequences.containsKey(name.toLowerCase()))
			throw new JDOException("Sequence " + name + "does not exist");
		sequences.remove(name.toLowerCase());
	}

	@Override
	public Sequence getSequence(String name) throws JDOException {
		if (sequences.containsKey(name.toLowerCase()))
			return sequences.get(name);
		else
			throw new JDOException("Sequence "+name+" not found");
	}

	@Override
	public Object call(String statement, Param... parameters) throws JDOException {
		throw new JDOUnsupportedOptionException("FileDataSource does not support callable statements");
	}

	@Override
	public boolean inTransaction() throws JDOException {
		return false;
	}

	@Override
	public void close() throws JDOException {
	}

	@Override
	public void createBucket(String bucketName, Map<String, Object> options) throws JDOException {
		FileBucket b = new FileBucket(getProperties(), bucketName, true);
		b.close();
	}

	@Override
	public FileBucket openBucket(String bucketName) throws JDOException {
		return new FileBucket(getProperties(), bucketName, false);
	}

	@Override
	public void dropBucket(String bucketName) throws JDOException {
		truncateBucket(bucketName);
		String path = getProperties().get(URI);
		if (!path.endsWith(File.separator))
			path += File.separator;
		File dir = new File(path+bucketName);
		if (dir.exists())
			dir.delete();
	}

	@Override
	public void truncateBucket(String bucketName) throws JDOException {
		String path = getProperties().get(URI);
		if (!path.endsWith(File.separator))
			path += File.separator;
		File dir = new File(path+bucketName);
		if (dir.exists()) {
			if (dir.isDirectory()) {
				for (File f : dir.listFiles())
					if (f.isFile())
						f.delete();
			} else {
				throw new JDOException(path + bucketName + " is not a directory");
			}
		}
	}
}
