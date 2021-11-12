package org.judal.bdbj;

/*
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

import java.io.Serializable;

import javax.jdo.JDOException;

import org.judal.metadata.TableDef;
import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.keyvalue.Stored;

/**
 * <p>Core class for persisting Java objects in Berkeley DB.<p>
 * <p>Although not an abstract class, DBEntity is designed to be inherited
 * by a child class implementing specific behavior for reading a writing a Java
 * object from and to Berkeley DB.</p>
 * <p>Berkeley DB is a key-value store with no metadata features for describing columns
 * inside the stored value. It is the responsibility of each DBEntity subclass to pass
 * the column list of the stored value as a parameter to the DBEntity constructor.
 * The column list will usually be obtained from a com.knowgate.storage.SchemaMetaData instance.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class DBJStored implements Stored {

  private static final long serialVersionUID = 600000101201000110l;

  private String key;
  private Object value;
  private String bucketName;

  public DBJStored(TableDef tblDef) {
	  this.bucketName = tblDef.getName();
  }

  @Override
  public void setKey(Object key) throws JDOException {
	  if (null==key)
	  	this.key = (String) key;
	  else if (key instanceof byte[])
		  this.key = new String((byte[]) key);
	  else if (key instanceof String)
		  this.key = (String) key;
	  else
		  this.key = key.toString();
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
  public boolean load(DataSource dataSource, Object key) throws JDOException {
	DBJBucket bck = null;
	boolean retval = false;
	try {
		bck = ((DBJBucketDataSource) dataSource).openBucket(getBucketName());
		retval = bck.load(key, this);
	} finally {
		if (bck!=null) bck.close();
	}
	return retval;
  }

  @Override
  public void store(DataSource dataSource) throws JDOException {
		DBJBucket bck = null;
		try {
			bck = ((DBJBucketDataSource) dataSource).openBucket(getBucketName());
			bck.store(this);
		} finally {
			if (bck!=null) bck.close();
		}
  }

  @Override
  public void delete(DataSource dataSource) throws JDOException {
		DBJBucket bck = null;
		try {
			bck = ((DBJBucketDataSource) dataSource).openBucket(getBucketName());
			bck.delete(this);
		} finally {
			if (bck!=null) bck.close();
		}
  }

  @Override
  public boolean load(Object key) throws JDOException {
	  return load(EngineFactory.getDefaultBucketDataSource(), key);
  }

  @Override
  public void store() throws JDOException {
	  store(EngineFactory.getDefaultBucketDataSource());
  }

  @Override
  public void delete() throws JDOException {
	  delete(EngineFactory.getDefaultBucketDataSource());
  }
  
}
