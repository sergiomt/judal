package org.judal.bdb;

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

import java.io.FileNotFoundException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.jdo.JDOException;
import javax.transaction.TransactionManager;

import org.judal.metadata.IndexDef.Type;
import org.judal.metadata.TableDef;
import org.judal.storage.BucketDataSource;

import com.sleepycat.db.DatabaseException;

public class DBBucketDataSource extends DBDataSource implements BucketDataSource {

	private static HashMap<String,TableDef> bucketDefs = new HashMap<String,TableDef>();
	
	public DBBucketDataSource(Map<String, String> properties, TransactionManager transactManager) throws JDOException {
		super(properties, transactManager, null);
	}

	public static TableDef getKeyValueDef(String bucketName) {
		if (!bucketDefs.containsKey(bucketName)) {
			TableDef bucketDef = new TableDef(bucketName);
			bucketDef.addColumnMetadata("", "key", Types.VARCHAR, 256, 0, false, Type.ONE_TO_ONE, null, null, true);
			bucketDef.addColumnMetadata("", "value", Types.LONGVARBINARY, Integer.MAX_VALUE, 0, false, null, null, null, false);
			bucketDefs.put(bucketName, bucketDef);
		}
		return bucketDefs.get(bucketName);
	}

	@Override
	public void createBucket(String bucketName, Map<String, Object> options) throws JDOException {
		Properties props = new Properties();
		if (null!=options)
			props.putAll(options);
		props.put("name",bucketName);
		DBBucket tbl = openTableOrBucket(props, getKeyValueDef(bucketName), null);
		tbl.close();
	}

	@Override
	public DBBucket openBucket(String bucketName) throws JDOException {
		Properties props = new Properties();
		props.put("name",bucketName);
		return openTableOrBucket(props, getKeyValueDef(bucketName), null);
	}

	@Override
	public void dropBucket(String bucketName) throws JDOException {		
		try {
			getEnvironment().removeDatabase(getTransaction(), getPath()+bucketName+".db", bucketName);
		} catch (FileNotFoundException | DatabaseException e) {
			throw new JDOException(e.getMessage(), getKeyValueDef(bucketName));
		}		
	}

	@Override
	public void truncateBucket(String bucketName) throws JDOException {
		DBBucket tbl = openBucket(bucketName);
		try {
			tbl.truncate();
		} finally {
			if (tbl!=null) tbl.close();
		}
	}

}
