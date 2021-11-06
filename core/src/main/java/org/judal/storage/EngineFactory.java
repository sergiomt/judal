package org.judal.storage;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.util.Hashtable;

import javax.jdo.JDOUserException;

import org.judal.storage.keyvalue.BucketDataSource;
import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.table.TableDataSource;

/**
 * <p>Engine registry and default data source keeper.</p>
 * This singleton serves to provide a central registry of storage engines in use and 
 * also as a holder of data sources that are used implicitly by load, store and fetch methods
 * when no data source is provided explicitly.
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class EngineFactory {

	/**
	 * A default data source can be kept per thread so that load and store methods can use it implicitly.
	 */
	public static ThreadLocal<DataSource> DefaultThreadDataSource = new ThreadLocal<DataSource>();
	
	private static Hashtable<String, Class<Engine<? extends DataSource>>> engines = new Hashtable<String, Class<Engine<? extends DataSource>>>();

	@SuppressWarnings("unchecked")
	/**
	 * <p>Register an Engine implementation under a given name</p>
	 * @param engineName String Engine name.
	 * @param engineClassName String Name of an implementation of org.judal.storage.Engine
	 * @throws ClassNotFoundException
	 * @throws IllegalArgumentException If another Engine implementation is already registered under the same name
	 */
	public static void registerEngine(String engineName, String engineClassName) throws ClassNotFoundException, IllegalArgumentException {
		Class<Engine<? extends DataSource>> engineClass = engines.get(engineName);
		if (engineClass==null)
			engines.put(engineName, (Class<Engine<? extends DataSource>>) Class.forName(engineClassName));
		else if (!engineClass.getName().equals(engineClassName))
			throw new IllegalArgumentException("EngineFactory.registerEngine() Engine " + engineName + " is already registered for class " + engineClassName);
	}
	
	/**
	 * <p>Deregister Engine.</p>
	 * @param engineName String Engine name.
	 * @throws IllegalArgumentException If no Engine is registered with given name.
	 */
	public static void deregisterEngine(String engineName) throws IllegalArgumentException{
		if (engines.containsKey(engineName))
			engines.remove(engineName);
		else
			throw new IllegalArgumentException("EngineFactory.deregisterEngine() Engine " + engineName + " is not registered");
	}

	/**
	 * <p>Get implementation for a given named Engine</p>
	 * @param engineName String
	 * @return Engine&lt;? extends DataSource&gt;
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws NullPointerException If engineName is <b>null</b> or empty String
	 * @throws IllegalArgumentException If no Engine has been previously registered with the given name
	 */
	public static Engine<? extends DataSource> getEngine(String engineName) throws InstantiationException, IllegalAccessException, NullPointerException, IllegalArgumentException {
		if (null==engineName)
			throw new NullPointerException("EngineFactory.getEngine() Engine name cannot be null");
		else if (engineName.length()==0)
			throw new NullPointerException("EngineFactory.getEngine() Engine name is required");
		Class<Engine<? extends DataSource>> engineClass = engines.get(engineName);
		if (null==engineClass)
			throw new IllegalArgumentException("EngineFactory.getEngine() Cannot find any Engine with name " + engineName);
		return engineClass.newInstance();
	}

	/**
	 * <p>Get default key-value data source.</p>
	 * Checks whether EngineFactory.DefaultThreadDataSource is instance of BucketDataSource
	 * if that is the case then return EngineFactory.DefaultThreadDataSource else if
	 * StorageContext.Default.getKeyValueDataSource() is not null then return key-value
	 * data source from StorageContext else throw JDOUserException if a default key-value
	 * data source can't be found at thread local nor StorageContext.
	 * @return BucketDataSource
	 * @throws JDOUserException If there is no default key-value data source set.
	 */
	public static BucketDataSource getDefaultBucketDataSource() throws JDOUserException {
		
		BucketDataSource tdts = null; 		
		DataSource ddts = EngineFactory.DefaultThreadDataSource.get();
		
		if (null==ddts)
			tdts = StorageContext.Default.getKeyValueDataSource();
		else if (ddts instanceof BucketDataSource)
			tdts = (BucketDataSource) ddts;
		else
			tdts = StorageContext.Default.getKeyValueDataSource();

		if (null==tdts)
			throw new JDOUserException("No suitable default BucketDataSource found at EngineFactory or StorageContext");
		
		return tdts;
	}

	/**
	 * <p>Get default table data source.</p>
	 * Checks whether EngineFactory.DefaultThreadDataSource is instance of TableDataSource
	 * if that is the case then return EngineFactory.DefaultThreadDataSource else if
	 * StorageContext.Default.getTableDataSource() is not null then return table
	 * data source from StorageContext else throw JDOUserException if a default table
	 * data source can't be found at thread local nor StorageContext.
	 * @return TableDataSource
	 * @throws JDOUserException If there is no default table data source set.
	 */
	public static TableDataSource getDefaultTableDataSource() throws JDOUserException {
		
		TableDataSource tdts = null; 		
		DataSource ddts = EngineFactory.DefaultThreadDataSource.get();

		if (null==ddts)
			tdts = StorageContext.Default.getTableDataSource();
		else if (ddts instanceof TableDataSource)
			tdts = (TableDataSource) ddts;
		else
			tdts = StorageContext.Default.getTableDataSource();

		if (null==tdts)
			tdts = StorageContext.Default.getRelationalDataSource();
			
		if (null==tdts)
			throw new JDOUserException("No suitable default TableDataSource found at EngineFactory or StorageContext");
		
		return tdts;
	}

	/**
	 * <p>Get default relational data source.</p>
	 * Checks whether EngineFactory.DefaultThreadDataSource is instance of RelationalDataSource
	 * if that is the case then return EngineFactory.DefaultThreadDataSource else if
	 * StorageContext.Default.getRelationalDataSource() is not null then return table
	 * data source from StorageContext else throw JDOUserException if a default relational
	 * data source can't be found at thread local nor StorageContext.
	 * @return RelationalDataSource
	 * @throws JDOUserException If there is no default relational data source set.
	 */
	public static RelationalDataSource getDefaultRelationalDataSource() throws JDOUserException {
		
		RelationalDataSource rdts = null; 
		DataSource ddts = EngineFactory.DefaultThreadDataSource.get();

		if (null==ddts)
			rdts = StorageContext.Default.getRelationalDataSource();
		else if (ddts instanceof RelationalDataSource)
			rdts = (RelationalDataSource) ddts;
		else
			rdts = StorageContext.Default.getRelationalDataSource();
			
		if (null==rdts)
			throw new JDOUserException("No suitable default RelationalDataSource found at EngineFactory or StorageContext");
		
		return rdts;
	}

	
	/*
	 * Some standard names for engines
	 */
	public static final String NAME_JDBC = "JDBC";
	public static final String NAME_BERKELEYDB = "BDB";
	public static final String NAME_BERKELEYDB_JAVA = "BDBJ";
	public static final String NAME_AMAZONS3 = "S3";
	public static final String NAME_CASSANDRA = "CASSANDRA";
	public static final String NAME_HBASE = "HBASE";
	public static final String NAME_MONGODB = "MONGODB";
	public static final String NAME_REDIS = "REDIS";
	public static final String NAME_FILE = "FILE";
	public static final String NAME_INMEMORY = "INMEMORY";
	public static final String NAME_HALODB = "HALODB";

}
