package org.judal.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.cache.Cache;
import javax.cache.Cache.Entry;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;

import org.judal.metadata.PrimaryKeyDef;
import org.judal.metadata.TableDef;
import org.judal.storage.queue.RecordQueueProducer;

import com.knowgate.debug.DebugFile;

public class RecordManager {

	private Properties properties;
	private Cache<Object, Record> cache;
	private TableDataSource dataSource;
	private RecordQueueProducer storageQueue;
	
	public RecordManager(TableDataSource dataSource, RecordQueueProducer storageQueue,
                         Cache<Object, Record> cache, Map<String,String> propsMap) {
		this.dataSource = dataSource;
		this.storageQueue = storageQueue;
		this.cache = cache;
		this.properties = new Properties();
		if (propsMap!=null)
			for (Map.Entry<String,String> e : propsMap.entrySet())
				properties.put(e.getKey(), e.getValue());
	}

	public void close() {
		cache.close();
		storageQueue.close();
		dataSource.close();
		dataSource = null;
	}	

	public void deletePersistent(Object obj) {
		Record rec = (Record) obj;
		evict(rec);
		storageQueue.delete(rec, new String[]{(String) rec.getKey()}, properties);
	}

	public void updatePersistent(Object obj, Param... params) {
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
		String[] keys = new String[objs.length];
		for (int k=0; k<objs.length; k++) {
			Record rec = (Record) objs[k];
			keys[k] = (String) rec.getKey();			
			evict(rec);
		}
		storageQueue.delete((Record) objs[0], keys, properties);
	}


	public void deletePersistentAll(Collection objs) {
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
		cache.remove(((Record) rec).getKey());
	}


	public void evictAll() {
		cache.clear();
	}


	public void evictAll(Object... objs) {
		for (Object obj : objs)
			evict(obj);
	}


	public void evictAll(Collection objs) {
		for (Object obj : objs)
			evict(obj);
	}


	public void evictAll(boolean subclasses, Class class1) {
		ArrayList<Object> evicted = new ArrayList<Object>(100);
		for (Entry<Object,Record> keyvalue: cache) {
			Object entry;
				entry = cache.get(keyvalue.getKey());
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

	public TableDataSource getDataSource() {
		return dataSource;
	}

	public JDOConnection getDataStoreConnection() {
		return dataSource.getJdoConnection();
	}

	public boolean getIgnoreCache() {
		return false;
	}


	public Object getObjectById(Object id) throws JDOUserException {
		Object obj = cache.get(id);
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
		return dataSource.getSequence(name);
	}


	public boolean isClosed() {
		return dataSource == null;
	}


	public <T> T makePersistent(T obj) {
		if (((Record) obj).getKey()==null)
			throw new NullPointerException("Record key may not be null");
		evict(obj);
		storageQueue.store((Record) obj);
		return obj; 
	}


	public <T> T[] makePersistentAll(T... objs) {
		Record[] recs = new Record[objs.length];
		int r = 0;
		for (T obj : objs) {
			recs[r++] = (Record) obj;
			evict(obj);
		}
		storageQueue.store(recs);
		return objs;
	}


	public <T> Collection<T> makePersistentAll(Collection<T> objs) {
		Record[] recs = new Record[objs.size()];
		int r = 0;
		for (T obj : objs) {
			recs[r++] = (Record) obj;
			evict(obj);
		}
		storageQueue.store(recs);
		return objs;
	}

	public Record newInstance(Class<? extends Record> recordClass, String tableName) throws NoSuchMethodException, JDOException {
		return StorageObjectFactory.newRecord(recordClass, dataSource.getMetaData().getTable(tableName));
	}


	@SuppressWarnings("unchecked")
	public void refresh(Object obj) {		
		Table tbl = null;
		Record former = (Record) obj;
		evict(obj);
		try {
			tbl = dataSource.openTable(former);
			if (tbl.load(former.getKey(), former))
				try {
					cache.put(former.getKey(), former);
				} catch (IllegalStateException | IllegalArgumentException | JDOException xcpt) {
					if (DebugFile.trace)
						DebugFile.writeln("TableManager.retrieve("+former.getKey()+") " + xcpt.getClass().getName() + " " + xcpt.getMessage());
				}
		} finally {
			tbl.close();
		}
	}

	public void retrieve(Object obj) {
		Record rec = (Record) obj;
		if (rec.getKey()==null)
			throw new JDOUserException("The supplied object has no value for its primary key");
		try {
			Record cached = (Record) getObjectById(rec.getKey());
			if (!cached.getTableName().equals(rec.getTableName()))
				throw new JDOUserException("Another object with the same id "+rec.getKey()+" but on a different table "+cached.getTableName()+" is already cached");
			rec.setValue((Serializable) cached.getValue()); 
		} catch (JDOUserException notfound) {
			Table tbl = null;
			try {
				tbl = dataSource.openTable((Record) obj);
				if (tbl.load(rec.getKey(), rec))
					try {
						cache.put(rec.getKey(), rec);
					} catch (IllegalStateException | IllegalArgumentException | JDOException xcpt) {
						if (DebugFile.trace)
							DebugFile.writeln("TableManager.retrieve("+rec.getKey()+") " + xcpt.getClass().getName() + " " + xcpt.getMessage());
					}
				else
					rec.setKey(null);
			} finally {
				tbl.close();
			}			
		}
	}

}
