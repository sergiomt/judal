package org.judal.storage;

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

import java.util.Map;

import org.judal.storage.DataSource;
import org.judal.storage.table.TableDataSource;
import org.judal.transaction.DataSourceTransactionManager;
import org.judal.storage.relational.RelationalDataSource;

public class StorageContext implements AutoCloseable {

	private DataSource keyValueDataSource = null;
	
	private TableDataSource tableDataSource = null;

	private RelationalDataSource relationalDataSource = null;
	
	public final static StorageContext Default = new StorageContext();

	public void initializeKeyValueDataSource(Engine<? extends DataSource> bucketEngine, Map<String,String> properties) throws IllegalStateException {
		if (null!=keyValueDataSource)
			throw new IllegalStateException("Key-Value Data Source has alredy been initialized");
		keyValueDataSource = bucketEngine.getDataSource(properties, null);
	}

	public void initializeKeyValueDataSource(Engine<? extends DataSource> bucketEngine, Map<String,String> properties, DataSourceTransactionManager transactionManager) throws IllegalStateException {
		if (null!=keyValueDataSource)
			throw new IllegalStateException("Key-Value Data Source has alredy been initialized");
		keyValueDataSource = bucketEngine.getDataSource(properties, transactionManager);
	}

	public DataSource getKeyValueDataSource() {
		return keyValueDataSource;
	}

	public void closeKeyValueDataSource() throws IllegalStateException {
		if (null!=keyValueDataSource)
			throw new IllegalStateException("Key-Value Data Source has not been initialized");
		try {
			keyValueDataSource.close();
		} catch (Exception e) {
			throw new IllegalStateException(e.getClass().getName()+" closing DataSource of StorageContext");
		}
		keyValueDataSource = null;
	}

	public void initializeTableDataSource(Engine<? extends TableDataSource> tableEngine, Map<String,String> properties) throws IllegalStateException {
		if (null!=tableDataSource)
			throw new IllegalStateException("Table Data Source has alredy been initialized");
		tableDataSource = tableEngine.getDataSource(properties, null);
	}

	public void initializeTableDataSource(Engine<? extends TableDataSource> tableEngine, Map<String,String> properties, DataSourceTransactionManager transactionManager) throws IllegalStateException {
		if (null!=tableDataSource)
			throw new IllegalStateException("Table Data Source has alredy been initialized");
		tableDataSource = tableEngine.getDataSource(properties, transactionManager);
	}

	public TableDataSource getTableDataSource() {
		return tableDataSource;
	}

	public void closeTableDataSource() throws IllegalStateException {
		if (null!=tableDataSource)
			throw new IllegalStateException("Table Data Source has not been initialized");
		try {
			tableDataSource.close();
		} catch (Exception e) {
			throw new IllegalStateException(e.getClass().getName()+" closing TableDataSource of StorageContext");
		}
		tableDataSource = null;
	}

	public void initializeRelationalDataSource(Engine<? extends RelationalDataSource> relationalEngine, Map<String,String> properties) throws IllegalStateException {
		if (null!=relationalDataSource)
			throw new IllegalStateException("Relational Data Source has alredy been initialized");
		relationalDataSource = relationalEngine.getDataSource(properties, null);
	}
	
	public void initializeRelationalDataSource(Engine<? extends RelationalDataSource> relationalEngine, Map<String,String> properties, DataSourceTransactionManager transactionManager) throws IllegalStateException {
		if (null!=relationalDataSource)
			throw new IllegalStateException("Relational Data Source has alredy been initialized");
		relationalDataSource = relationalEngine.getDataSource(properties, transactionManager);
	}

	public RelationalDataSource getRelationalDataSource() {
		return relationalDataSource;
	}

	public void closeRelationalDataSource() throws IllegalStateException {
		if (null!=relationalDataSource)
			throw new IllegalStateException("Relational Data Source has not been initialized");
		try {
			relationalDataSource.close();
		} catch (Exception e) {
			throw new IllegalStateException(e.getClass().getName()+" closing RelationalDataSource of StorageContext");
		}
		relationalDataSource = null;
	}

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
