package org.judal.halodb;

/**
 * © Copyright 2016 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.util.*;

import javax.transaction.TransactionManager;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.datastore.JDOConnection;

import com.oath.halodb.HaloDB;
import com.oath.halodb.HaloDBException;
import com.oath.halodb.HaloDBOptions;

import org.judal.serialization.BytesConverter;
import org.judal.storage.Param;
import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.BucketDataSource;
import org.judal.storage.keyvalue.Stored;

/**
 * HaloDB implementation of BucketDataSource
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class HaloDBDataSource implements BucketDataSource {

	private final Map<String,String> props;
	private HaloDB hdb;

	/**
	 * 
	 * @param properties Map&lt;String,String&gt;
	 */
	public HaloDBDataSource(Map<String,String> properties) throws HaloDBException {

		if (!properties.containsKey(DIRECTORY)) {
			throw new RuntimeException(DIRECTORY + " property is required");
		}

		final HaloDBOptions options = new HaloDBOptions();
		final String directory = properties.get(DIRECTORY);
		Integer intVal;
		Boolean boolVal;

		// Size of each data file in bytes
		intVal = getInt(properties, MAX_FILE_SIZE);
		if (intVal!=null)
			options.setMaxFileSize(intVal);

		// Size of each tombstone file in bytes
		// Large file size mean less file count but will slow down db open time.
		// But if set file size too small, it will result large amount of tombstone files under db folder
		intVal = getInt(properties, MAX_TOMBSTONE_FILE_SIZE);
		if (intVal!=null)
			options.setMaxTombstoneFileSize(intVal);

		// Set the number of threads used to scan index and tombstone files in parallel
		// to build in-memory index during db open. It must be a positive number which is
		// not greater than Runtime.getRuntime().availableProcessors().
		// It is used to speed up db open time.
		intVal = getInt(properties, INDEX_THREADS);
		if (intVal!=null)
			options.setBuildIndexThreads(intVal);

		// The threshold at which page cache is synced to disk.
		// data will be durable only if it is flushed to disk, therefore
		// more data will be lost if this value is set too high. Setting
		// this value too low might interfere with read and write performance.
		intVal = getInt(properties, FLUSH_SIZE_BYTES);
		if (intVal!=null)
			options.setFlushDataSizeBytes(intVal);

		// The percentage of stale data in a data file at which the file will be compacted.
		// This value helps control write and space amplification. Increasing this value will
		// reduce write amplification but will increase space amplification.
		// This along with the compactionJobRate below is the most important setting
		// for tuning HaloDB performance. If this is set to x then write amplification will be approximately 1/x.
		intVal = getInt(properties, COMPACTION_THRESHOLD);
		if (intVal!=null)
			options.setCompactionThresholdPerFile(intVal);

		// Controls how fast the compaction job should run.
		// This is the amount of data which will be copied by the compaction thread per second.
		// Optimal value depends on the compactionThresholdPerFile option.
		intVal = getInt(properties, COMPACTION_JOB_RATE);
		if (intVal!=null)
			options.setCompactionJobRate(intVal);

		// Setting this value is important as it helps to preallocate enough
		// memory for the off-heap cache. If the value is too low the db might
		// need to rehash the cache. For a db of size n set this value to 2*n.
		intVal = getInt(properties, NUMBER_OF_RECORDS);
		if (intVal!=null)
			options.setNumberOfRecords(intVal);

		// Delete operation for a key will write a tombstone record to a tombstone file.
		// the tombstone record can be removed only when all previous version of that key
		// has been deleted by the compaction job.
		// enabling this option will delete during startup all tombstone records whose previous
		// versions were removed from the data file.
		boolVal = getBool(properties, CLEANUP_TOMBSTONE);
		if (boolVal!=null)
			options.setCleanUpTombstonesDuringOpen(boolVal);

		// HaloDB does native memory allocation for the in-memory index.
		// Enabling this option will release all allocated memory back to the kernel when the db is closed.
		// This option is not necessary if the JVM is shutdown when the db is closed, as in that case
		// allocated memory is released automatically by the kernel.
		// If using in-memory index without memory pool this option,
		// depending on the number of records in the database,
		// could be a slow as we need to call _free_ for each record.
		boolVal = getBool(properties, CLEANUP_MEMORY);
		if (boolVal!=null)
			options.setCleanUpInMemoryIndexOnClose(boolVal);

		// Use memory pool
		boolVal = getBool(properties, USEPOOL);
		if (boolVal!=null)
			options.setUseMemoryPool(boolVal);

		// Hash table implementation in HaloDB is similar to that of ConcurrentHashMap in Java 7.
		// Hash table is divided into segments and each segment manages its own native memory.
		// The number of segments is twice the number of cores in the machine.
		// A segment's memory is further divided into chunks whose size can be configured here.
		intVal = getInt(properties, POOLSIZE);
		if (intVal!=null)
			options.setMemoryPoolChunkSize(intVal);

		// using a memory pool requires us to declare the size of keys in advance.
		// Any write request with key length greater than the declared value will fail, but it
		// is still possible to store keys smaller than this declared size.
		intVal = getInt(properties, KEY_SIZE);
		if (intVal!=null)
			options.setFixedKeySize(intVal);

		// Open the database. Directory will be created if it doesn't exist.
		// If we are opening an existing database HaloDB needs to scan all the
		// index files to create the in-memory index, which, depending on the db size, might take a few minutes.
		hdb = HaloDB.open(directory, options);

		props = new HashMap<>();
		props.putAll(properties);
	}

	/**
	 * This method always returns true
	 * @param objectName String Ignored
	 * @param objectType String Ignored
	 * @return boolean true
	 */
	@Override
	public boolean exists(String objectName, String objectType) throws JDOException {
		return true;
	}

	/**
	 * return Map&lt;String,String&gt;
	 */
	@Override
	public Map<String, String> getProperties() {
		return props;
	}

	/**
	 * This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException HaloDBDataSource does not support transactions
	 */
	@Override
	public TransactionManager getTransactionManager() {
		throw new JDOUnsupportedOptionException("HaloDBDataSource does not support transactions");
	}

	/**
	 * This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException HaloDBDataSource does not support transactions
	 */
	@Override
	public boolean inTransaction() throws JDOException {
		throw new JDOUnsupportedOptionException("HaloDBDataSource does not support transactions");
	}

	/**
	 *
	 * @throws JDOException Wrapper for HaloDBException
	 */
	@Override
	public void close() throws JDOException {
		if (hdb!=null) {
			try {
				hdb.close();
				hdb = null;
			} catch (HaloDBException e) {
				throw new JDOException(e.getMessage(), e);
			}
		}
	}

	/**
	 * This method does nothing.
	 * @param bucketName String
	 * @param options Map&lt;String,Object&gt; Unused
	 * @throws JDOException Never actually thrown
	 */
	@Override
	public void createBucket(String bucketName, Map<String,Object> options) throws JDOException {
		// Do nothing
	}

	/**
	 * @param bucketName String Bucket Name
	 * @return HaloDBBucket
	 * @throws JDOException Never actually thrown
	 */
	@Override
	public Bucket openBucket(String bucketName) throws JDOException {
		return new HaloDBBucket(this, bucketName);
	}

	/**
	 * Delete all objects in a given Bucket.
	 * @param bucketName String
	 * @throws JDOException Wrapper for HaloDBException or IOException
	 */
	@Override
	public void dropBucket(String bucketName) throws JDOException {
		truncateBucket(bucketName);
	}

	/**
	 * Delete all objects in a given Bucket.
	 * @param bucketName String
	 * @throws JDOException Wrapper for HaloDBException
	 */
	@Override
	public void truncateBucket(String bucketName) throws JDOException {
		final List<byte[]> keys = new ArrayList<>(1000);
		try {
			for (Stored stored : new HaloDBBucket(this, bucketName)) {
				keys.add(BytesConverter.toBytes(stored.getKey()));
			}
			for (byte[] key : keys) {
				hdb.delete(key);
			}
		} catch (HaloDBException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	/**
	 * This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException HaloDBDataSource does not use connections
	 */
	@Override
	public JDOConnection getJdoConnection() throws JDOException {
		throw new JDOUnsupportedOptionException("HaloDBDataSource does not use connections");
	}

	/**
	 * This method will always raise JDOUnsupportedOptionException
	 * @return No return value, always throws an exception
	 * @throws JDOUnsupportedOptionException HaloDBDataSource does not support sequences
	 */
	@Override
	public javax.jdo.datastore.Sequence getSequence(String name) throws JDOException {
		throw new JDOUnsupportedOptionException("HaloDBDataSource does not support sequences");
	}

	/**
	 * This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException HaloDBDataSource does not provide callable statements
	 */
	@Override
	public Object call(String statement, Param... parameters) throws JDOException {
		throw new JDOUnsupportedOptionException("InMemoryDataSource does not support callable statements");
	}

	/**
	 *
	 * @return HaloDB
	 */
	public HaloDB getDatabase() {
		return hdb;
	}

	private Integer getInt(Map<String,String> properties, String propertyName) {
		Integer propertyValue;
		if (properties.containsKey(propertyName)) {
			try {
				propertyValue = Integer.parseInt(propertyName);
			} catch (NumberFormatException nfe) {
				propertyValue = null;
			}
		} else {
			propertyValue = null;
		}
		return propertyValue;
	}

	private Boolean getBool(Map<String,String> properties, String propertyName) {
		return properties.containsKey(propertyName) ? Boolean.parseBoolean(propertyName) : null;
	}

}
