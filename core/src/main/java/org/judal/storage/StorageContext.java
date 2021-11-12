package org.judal.storage;

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

import java.util.Map;

import org.judal.storage.DataSource;
import org.judal.storage.keyvalue.BucketDataSource;
import org.judal.storage.table.TableDataSource;
import org.judal.transaction.DataSourceTransactionManager;
import org.judal.storage.relational.RelationalDataSource;

/**
 * <p>Container for a set of data sources used together by a client application.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class StorageContext implements AutoCloseable {

	private BucketDataSource keyValueDataSource = null;
	
	private TableDataSource tableDataSource = null;

	private RelationalDataSource relationalDataSource = null;
	
	public final static StorageContext Default = new StorageContext();

	/**
	 * <p>Create an initialize a non-transactional key-value data source from a map of configuration properties.</p>
	 * Properties must be as listed in org.judal.storage.DataSource static variables.
	 * @param bucketEngine Engine&lt;? extends BucketDataSource&gt;
	 * @param properties Map&lt;String,String;&gt;
	 * @throws IllegalStateException if the key-value data source for this StorageContext has already been initialized.
	 */
	public void initializeKeyValueDataSource(Engine<? extends BucketDataSource> bucketEngine, Map<String,String> properties) throws IllegalStateException {
		if (null!=keyValueDataSource)
			throw new IllegalStateException("Key-Value Data Source has already been initialized");
		keyValueDataSource = (BucketDataSource) bucketEngine.getDataSource(properties, null);
	}

	/**
	 * <p>Create an initialize a transactional key-value data source from a map of configuration properties.</p>
	 * Properties must be as listed in org.judal.storage.DataSource static variables.
	 * @param bucketEngine Engine&lt;? extends DataSource&gt;
	 * @param properties Map&lt;String,String;&gt;
	 * @param transactionManager DataSourceTransactionManager
	 * @throws IllegalStateException if the key-value data source for this StorageContext has already been initialized.
	 */
	public void initializeKeyValueDataSource(Engine<? extends DataSource> bucketEngine, Map<String,String> properties, DataSourceTransactionManager transactionManager) throws IllegalStateException {
		if (null!=keyValueDataSource)
			throw new IllegalStateException("Key-Value Data Source has alredy been initialized");
		keyValueDataSource = (BucketDataSource) bucketEngine.getDataSource(properties, transactionManager);
	}

	/**
	 * @return BucketDataSource
	 */
	public BucketDataSource getKeyValueDataSource() {
		return keyValueDataSource;
	}

	/**
	 * <p>Close key-value data source held by this StorageContext.</p>
	 * @throws IllegalStateException if key-value data source has not been initialized
	 */
	public void closeKeyValueDataSource() throws IllegalStateException {
		if (null==keyValueDataSource)
			throw new IllegalStateException("Key-Value Data Source has not been initialized");
		try {
			keyValueDataSource.close();
			keyValueDataSource = null;
		} catch (Exception e) {
			throw new IllegalStateException(e.getClass().getName()+" closing DataSource of StorageContext");
		}
	}

	/**
	 * <p>Create an initialize a non-transactional table data source from a map of configuration properties.</p>
	 * Properties must be as listed in org.judal.storage.DataSource static variables.
	 * @param tableEngine Engine&lt;? extends TableDataSource&gt;
	 * @param properties Map&lt;String,String;&gt;
	 * @throws IllegalStateException if the table data source for this StorageContext has already been initialized.
	 */
	public void initializeTableDataSource(Engine<? extends TableDataSource> tableEngine, Map<String,String> properties) throws IllegalStateException {
		if (null!=tableDataSource)
			throw new IllegalStateException("Table Data Source has alredy been initialized");
		tableDataSource = tableEngine.getDataSource(properties, null);
	}

	/**
	 * <p>Create an initialize a transactional table data source from a map of configuration properties.</p>
	 * Properties must be as listed in org.judal.storage.DataSource static variables.
	 * @param tableEngine Engine&lt;? extends TableDataSource&gt;
	 * @param properties Map&lt;String,String;&gt;
	 * @param transactionManager DataSourceTransactionManager
	 * @throws IllegalStateException if the table data source for this StorageContext has already been initialized.
	 */
	public void initializeTableDataSource(Engine<? extends TableDataSource> tableEngine, Map<String,String> properties, DataSourceTransactionManager transactionManager) throws IllegalStateException {
		if (null!=tableDataSource)
			throw new IllegalStateException("Table Data Source has alredy been initialized");
		tableDataSource = tableEngine.getDataSource(properties, transactionManager);
	}

	/**
	 * @return TableDataSource
	 */
	public TableDataSource getTableDataSource() {
		return tableDataSource;
	}

	/**
	 * <p>Close table data source held by this StorageContext.</p>
	 * @throws IllegalStateException if table data source has not been initialized
	 */
	public void closeTableDataSource() throws IllegalStateException {
		if (null==tableDataSource)
			throw new IllegalStateException("Table Data Source has not been initialized");
		try {
			tableDataSource.close();
			tableDataSource = null;
		} catch (Exception e) {
			throw new IllegalStateException(e.getClass().getName()+" closing TableDataSource of StorageContext");
		}
	}

	/**
	 * <p>Create an initialize a non-transactional relational data source from a map of configuration properties.</p>
	 * Properties must be as listed in org.judal.storage.DataSource static variables.
	 * @param relationalEngine Engine&lt;? extends RelationalDataSource&gt;
	 * @param properties Map&lt;String,String;&gt;
	 * @throws IllegalStateException if the relational data source for this StorageContext has already been initialized.
	 */
	public void initializeRelationalDataSource(Engine<? extends RelationalDataSource> relationalEngine, Map<String,String> properties) throws IllegalStateException {
		if (null!=relationalDataSource)
			throw new IllegalStateException("Relational Data Source has alredy been initialized");
		relationalDataSource = relationalEngine.getDataSource(properties, null);
	}
	
	/**
	 * <p>Create an initialize a transactional relational data source from a map of configuration properties.</p>
	 * Properties must be as listed in org.judal.storage.DataSource static variables.
	 * @param relationalEngine Engine&lt;? extends RelationalDataSource&gt;
	 * @param properties Map&lt;String,String;&gt;
	 * @param transactionManager DataSourceTransactionManager
	 * @throws IllegalStateException if the relational data source for this StorageContext has already been initialized.
	 */
	public void initializeRelationalDataSource(Engine<? extends RelationalDataSource> relationalEngine, Map<String,String> properties, DataSourceTransactionManager transactionManager) throws IllegalStateException {
		if (null!=relationalDataSource)
			throw new IllegalStateException("Relational Data Source has alredy been initialized");
		relationalDataSource = relationalEngine.getDataSource(properties, transactionManager);
	}

	/**
	 * @return RelationalDataSource
	 */
	public RelationalDataSource getRelationalDataSource() {
		return relationalDataSource;
	}

	/**
	 * <p>Close relational data source held by this StorageContext.</p>
	 * @throws IllegalStateException if relational data source has not been initialized
	 */
	public void closeRelationalDataSource() throws IllegalStateException {
		if (null==relationalDataSource)
			throw new IllegalStateException("Relational Data Source has not been initialized");
		try {
			relationalDataSource.close();
			relationalDataSource = null;
		} catch (Exception e) {
			throw new IllegalStateException(e.getClass().getName()+" closing RelationalDataSource of StorageContext");
		}
	}

	/**
	 * <p>Close all the data sources held by this StorageContext.</p>
	 */
	@Override
	public void close() throws Exception {
		try  {
			if (null!=relationalDataSource) {
				relationalDataSource.close();
				relationalDataSource = null;
			}
		} catch (Exception xcpt) { }
		try  {
			if (null!=tableDataSource) {
				tableDataSource.close();
				tableDataSource = null;
			}
		} catch (Exception xcpt) { }
		try  {
			if (null!=keyValueDataSource) {
				keyValueDataSource.close();
				keyValueDataSource = null;
			}
		} catch (Exception xcpt) { }
	}

}
