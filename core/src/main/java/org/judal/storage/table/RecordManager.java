package org.judal.storage.table;

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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.cache.Cache;
import javax.cache.Cache.Entry;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;

import org.judal.metadata.PrimaryKeyDef;
import org.judal.metadata.TableDef;
import org.judal.storage.Param;
import org.judal.storage.StorageObjectFactory;
import org.judal.storage.queue.RecordQueueProducer;

import com.knowgate.debug.DebugFile;

public class RecordManager implements AutoCloseable {

	private final long MAX_WAIT_FOR_CACHE_STARTUP =  2;

	private final Properties properties;
	private final Future<Cache<Object, Record>> cache;
	private final TableDataSource dataSource;
	private final RecordQueueProducer storageQueue;
	private boolean closed;
	private boolean usingCache;
	private boolean enableCache;

	public RecordManager(final TableDataSource dataSource, final RecordQueueProducer storageQueue,
							final Future<Cache<Object, Record>> cache, final Map<String,String> propsMap) {
		if (null==dataSource)
			throw new NullPointerException("RecordManager constructor dataSource may not be null");
		if (null==storageQueue)
			throw new NullPointerException("RecordManager constructor storageQueue may not be null");
		this.dataSource = dataSource;
		this.storageQueue = storageQueue;
		this.cache = cache;
		this.usingCache = (cache!=null);
		this.enableCache = usingCache;
		this.properties = new Properties();
		if (propsMap!=null)
			for (Map.Entry<String,String> e : propsMap.entrySet())
				properties.put(e.getKey(), e.getValue());
		closed = false;
	}

	public RecordManager(final TableDataSource dataSource, final RecordQueueProducer storageQueue,
			final Cache<Object, Record> cache, final Map<String,String> propsMap) {
		if (null==dataSource)
			throw new NullPointerException("RecordManager constructor dataSource may not be null");
		if (null==storageQueue)
			throw new NullPointerException("RecordManager constructor storageQueue may not be null");
		this.dataSource = dataSource;
		this.storageQueue = storageQueue;
		this.usingCache = (cache!=null);
		this.enableCache = usingCache;
		FutureTask<Cache<Object, Record>> dummyTask = new FutureTask<Cache<Object, Record>>(new Runnable() { public void run() {}; },cache);
		dummyTask.run();
		this.cache = dummyTask;
		this.properties = new Properties();
		if (propsMap!=null)
			for (Map.Entry<String,String> e : propsMap.entrySet())
				properties.put(e.getKey(), e.getValue());
		closed = false;
	}

	private Cache<Object, Record> getCache() throws IllegalStateException {
		if (closed)
			throw new IllegalStateException("RecordManager.getCache() RecordManager has been closed");
		Cache<Object, Record> retval = null;
		try {
			retval = cache.get(MAX_WAIT_FOR_CACHE_STARTUP, TimeUnit.MINUTES);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			if (DebugFile.trace) {
				DebugFile.writeln("RecordManager.getCache() " +  e.getClass().getName() + " " + e.getMessage());
			}
		}
		return retval;
	}

	@Override
	public void close() {
		if (null!=getCache())
			getCache().close();
		usingCache = enableCache = false;
		storageQueue.close();
		dataSource.close();
		closed = true;
	}	

	/**
	 * Get whether this RecordManager is capable of keeping records in cache 
	 * @return boolean
	 */
	public boolean isCachingAvailable() {
		return usingCache;
	}

	/**
	 * @return boolean
	 */
	public boolean getCacheEnabled() {
		return usingCache && enableCache;
	}

	/**
	 * Enable or disable cache
	 * @param enable boolean
	 * @throws IllegalStateException If no caching provider was specified at constructor
	 */
	public void setCacheEnabled(final boolean enable) throws IllegalStateException {
		if (closed)
			throw new IllegalStateException("RecordManager.evictAll() RecordManager has been closed");
		if (!usingCache && enable)
			throw new IllegalStateException("RecordManager cannot enable cache because no caching provider was injected at constructoor");
		if (!enable && (getCache()!=null))
			getCache().clear();
		enableCache = enable;
	}

	public void deletePersistent(Object obj) {
		if (closed)
			throw new IllegalStateException("RecordManager.deletePersistent() RecordManager has been closed");
		Record rec = (Record) obj;
		evict(rec);
		storageQueue.delete(rec, new String[]{rec.getKey().toString()}, properties);
	}

	public void updatePersistent(Object obj, Param... params) {
		if (closed)
			throw new IllegalStateException("RecordManager.updatePersistent() RecordManager has been closed");
		Record rec = (Record) obj;
		evict(rec);
		TableDef tdef = dataSource.getMetaData().getTable(rec.getTableName());
		PrimaryKeyDef pk = tdef.getPrimaryKeyMetadata();
		if (pk.getNumberOfColumns()<1) {
			throw new JDOException("Cannot update table "+rec.getTableName()+" because it lacks of a primary key");
		} else {
			Param[] values = new Param[pk.getNumberOfColumns()];
			for (int p=0; p<pk.getNumberOfColumns(); p++)
				values[p] = new Param(tdef.getColumnByName(pk.getColumns()[p].getName()), rec.apply(pk.getColumns()[p].getName()));
			storageQueue.update(rec, params, values);
		}
	}

	public void deletePersistentAll(Object... objs) {
		if (closed)
			throw new IllegalStateException("RecordManager.deletePersistentAll() RecordManager has been closed");
		String[] keys = new String[objs.length];
		for (int k=0; k<objs.length; k++) {
			Record rec = (Record) objs[k];
			keys[k] = (String) rec.getKey();			
			evict(rec);
		}
		storageQueue.delete((Record) objs[0], keys, properties);
	}


	public void deletePersistentAll(@SuppressWarnings("rawtypes") Collection objs) {
		if (closed)
			throw new IllegalStateException("RecordManager.deletePersistentAll() RecordManager has been closed");
		if (objs.size()>0) {
			String[] keys = new String[objs.size()];
			int k = 0;
			Record rec = null;
			for (Object obj : objs) {
				rec = (Record) obj;
				keys[k] = (String) rec.getKey();			
				evict(rec);
			}
			storageQueue.delete(rec, keys, properties);			
		}
	}


	public void evict(Object rec) {
		if (closed)
			throw new IllegalStateException("RecordManager.evict() RecordManager has been closed");
		Object key = ((Record) rec).getKey();
		if (null==key)
			throw new NullPointerException("RecordManager.evict() Object key may not be null");
		if (getCacheEnabled())
			getCache().remove(key);
	}

	public void evictAll() {
		if (closed)
			throw new IllegalStateException("RecordManager.evictAll() RecordManager has been closed");
		if (getCacheEnabled())
			getCache().clear();
	}


	public void evictAll(Object... objs) {
		for (Object obj : objs)
			evict(obj);
	}


	public void evictAll(@SuppressWarnings("rawtypes") Collection objs) {
		for (Object obj : objs)
			evict(obj);
	}

	public void evictAll(boolean subclasses, @SuppressWarnings("rawtypes") Class class1) {
		if (getCacheEnabled()) {
			ArrayList<Object> evicted = new ArrayList<Object>(100);
			for (Entry<Object,Record> keyvalue: getCache()) {
				Object entry;
				entry = getCache().get(keyvalue.getKey());
				if (entry != null) {
					if (subclasses) {
						if (class1.isInstance(entry))
							evicted.add(entry);
					} else {
						if (class1.getName().equals(entry.getClass().getName()))
							evicted.add(entry);
					}
				}
			}
			evictAll(evicted);
		}
	}

	public TableDataSource getDataSource() {
		return closed ?  null : dataSource;
	}

	public JDOConnection getDataStoreConnection() {
		return closed ?  null : dataSource.getJdoConnection();
	}

	public boolean getIgnoreCache() {
		return false;
	}

	/**
	 * Get an object from local cache
	 * @param id Object
	 * @return Object
	 * @throws IllegalStateException If this RecordManager has been closed or is not using cache
	 * @throws JDOUserException if no object with the given id is found at the cache
	 */
	public Object getObjectById(Object id) throws IllegalStateException, JDOUserException {
		if (closed)
			throw new IllegalStateException("RecordManager.getObjectById() RecordManager has been closed");
		if (!getCacheEnabled())
			throw new IllegalStateException("RecordManager.getObjectById() RecordManager is not using caching");
		Object obj = getCache().get(id);
		if (null==obj)
			throw new JDOUserException("Object "+id+" not found in cache");
		return obj;
	}

	public Map<String, Object> getProperties() {
		HashMap<String, Object> props  = new HashMap<String, Object>();
		for (Object key : properties.keySet())
			props.put((String) key, properties.get(key));
		return Collections.unmodifiableMap(props);
	}

	public Sequence getSequence(String name) {
		if (closed)
			throw new IllegalStateException("RecordManager.getSequence() RecordManager has been closed");
		return dataSource.getSequence(name);
	}


	public boolean isClosed() {
		return closed;
	}

	public <T> T makePersistent(T obj) {
		if (closed)
			throw new IllegalStateException("RecordManager.makePersistent() RecordManager has been closed");
		if (((Record) obj).getKey()!=null)
			evict(obj);
		storageQueue.store((Record) obj);
		return obj; 
	}

	public <T> T[] makePersistentAll(@SuppressWarnings("unchecked") T... objs) {
		
		if (closed)
			throw new IllegalStateException("RecordManager.makePersistentAll() RecordManager has been closed");

		if (DebugFile.trace) {
			StringBuilder objClss = new StringBuilder();
			if (objs!=null)
				for (T obj : objs)
					objClss.append(objClss.length()==0 ? "" : ",").append(obj.getClass().getName());
			DebugFile.writeln("Begin RecordManager.makePersistentAll("+objClss.toString()+")");
			DebugFile.incIdent();
		}

		Record[] recs = new Record[objs.length];
		int r = 0;
		for (T obj : objs) {
			Record rec = (Record) obj;
			recs[r++] = rec;
			if (rec.getKey()!=null)
				evict(obj);
		}
		storageQueue.store(recs);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End RecordManager.makePersistentAll()");
		}

		return objs;
	}

	public <T> Collection<T> makePersistentAll(Collection<T> objs) {

		if (closed)
			throw new IllegalStateException("RecordManager.makePersistentAll() RecordManager has been closed");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin RecordManager.makePersistentAll(Collection<T>)");
			DebugFile.incIdent();
		}

		Record[] recs = new Record[objs.size()];
		int r = 0;
		for (T obj : objs) {
			Record rec = (Record) obj;
			recs[r++] = rec;
			if (rec.getKey()!=null)
				evict(obj);
		}
		storageQueue.store(recs);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End RecordManager.makePersistentAll()");
		}

		return objs;
	}

	public Record newInstance(Class<? extends Record> recordClass, String tableName) throws NoSuchMethodException, JDOException {
		return StorageObjectFactory.newRecord(recordClass, dataSource.getMetaData().getTable(tableName));
	}

	public void refresh(Object obj) {		
		if (getCacheEnabled()) {
			Table tbl = null;
			Record former = (Record) obj;
			evict(obj);
			try {
				tbl = dataSource.openTable(former);
				if (tbl.load(former.getKey(), former))
					try {
						getCache().put(former.getKey(), former);
					} catch (IllegalStateException | IllegalArgumentException | JDOException xcpt) {
						if (DebugFile.trace)
							DebugFile.writeln("TableManager.retrieve("+former.getKey()+") " + xcpt.getClass().getName() + " " + xcpt.getMessage());
					}
			} finally {
				tbl.close();
			}
		}
	}

	public void retrieve(Object obj) {
		Record rec = (Record) obj;
		if (rec.getKey()==null)
			throw new JDOUserException("The supplied object has no value for its primary key");
		if (getCacheEnabled()) {
			try {
				Record cached = (Record) getObjectById(rec.getKey());
				if (!cached.getTableName().equals(rec.getTableName()))
					throw new JDOUserException("Another object with the same id "+rec.getKey()+" but on a different table "+cached.getTableName()+" is already cached");
				rec.setValue((Serializable) cached.getValue()); 
			} catch (JDOUserException notfound) {
				try (Table tbl = dataSource.openTable((Record) obj)) {
					if (tbl.load(rec.getKey(), rec))
						try {
							getCache().put(rec.getKey(), rec);
						} catch (IllegalStateException | IllegalArgumentException | JDOException xcpt) {
							if (DebugFile.trace)
								DebugFile.writeln("TableManager.retrieve("+rec.getKey()+") " + xcpt.getClass().getName() + " " + xcpt.getMessage());
						}
					else
						rec.setKey(null);
				}
			}
		} else {
			try (Table tbl = dataSource.openTable((Record) obj)) {
				if (!tbl.load(rec.getKey(), rec))
					rec.setKey(null);
			}
		}
	}

	public void waitFor(final Record rec, final long maxWait, final long retries) throws IllegalArgumentException, InterruptedException {
		if (maxWait<0)
			throw new IllegalArgumentException("RecordManager.waitFor() maxWait must be equal to or greater than zero" );
		if (retries<0)
			throw new IllegalArgumentException("RecordManager.waitFor() retries must be equal to or greater than zero" );
		if (retries>maxWait)
			throw new IllegalArgumentException("RecordManager.waitFor() retries must less than or equals to maxWait" );
		long delay = retries==0 ? maxWait : maxWait / (retries+1l);
		boolean exists = false;
		for (long waiting = 0l; waiting<=maxWait && !exists; waiting+=delay) {
			try (View tbl = dataSource.openView(rec)) {
				exists = tbl.exists(rec.getKey());
			}
			Thread.sleep(delay);
		}
		if (!exists)
			throw new InterruptedException(rec.getClass().getName()+" not found " + rec.getKey() + " after " + retries + " in " + maxWait + " milliseconds");
	}
}