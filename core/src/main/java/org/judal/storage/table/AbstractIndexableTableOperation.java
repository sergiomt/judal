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

import javax.jdo.JDOException;

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

import org.judal.storage.EngineFactory;
import org.judal.storage.Param;
import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;

/**
 * <p>Base class for indexable table operation wrapper.</p>
 * Be sure to always call close() after an operation is done or else a connection leak may occur.
 * @author Sergio Montoro Ten
 * @param &lt;R extends Record&gt;
 * @version 1.0
 */
public abstract class AbstractIndexableTableOperation<R extends Record> extends AbstractTableOperation<R> {

	/**
	 * <p>Constructor.</p>
	 * Create IndexableTableOperation using EngineFactory default table data source.
	 */
	public AbstractIndexableTableOperation() {
		this(EngineFactory.getDefaultTableDataSource());
	}

	/**
	 * <p>Constructor.</p>
	 * Create IndexableTableOperation using EngineFactory default table data source.
	 * @param record R Instance of Record subclass to be used by this operation.
	 */
	public AbstractIndexableTableOperation(R record) {
		this(EngineFactory.getDefaultTableDataSource(), record);
	}

	/**
	 * <p>Constructor.</p>
	 * Create IndexableTableOperation using given table data source.
	 * @param dataSource TableDataSource
	 */
	public AbstractIndexableTableOperation(TableDataSource dataSource) {
		super(dataSource);
	}

	/**
	 * <p>Constructor.</p>
	 * Create IndexableTableOperation using given table data source.
	 * @param dataSource TableDataSource
	 * @param record R Instance of Record subclass to be used by this operation.
	 */
	public AbstractIndexableTableOperation(TableDataSource dataSource, R record) {
		super(dataSource, record);
	}

	@Override
	protected void open() {
		tbl = ((TableDataSource) dts).openIndexedTable(getRecord());
	}

	/**
	 * @return IndexableTable
	 */
	@Override
	public IndexableTable getTable() {
		return (IndexableTable) tbl;
	}

	/**
	 * <p>Get count of records with a given value in the specified column.</p>
	 * @param columnName String
	 * @param valueSearched Object
	 * @return long
	 * @throws JDOException
	 */
	public long count(String columnName, Object valueSearched) throws JDOException {
		return getTable().count(columnName, valueSearched);
	}

	/**
	 * <p>Check if exists record with the given values for its columns.</p>
	 * @param keys Param&hellip; Each Param name must match the desired column name for filtering.
	 * @return boolean
	 */
	public boolean exists(Param... keys) {
		return getTable().exists(keys);
	}

	/**
	 * <p>Fetch records.</p>
	 * @param maxrows int
	 * @param offset int
	 * @param keys Param&hellip; Each Param name must match the desired column name for filtering.
	 * @return Object an Iterable which type depends on whether the implementation is done by the Java or by the Scala adaptor.
	 */
	public abstract Object fetch(final int maxrows, final int offset, Param... keys);

	public int update(Param[] values, Param[] where) {
		return getTable().update(values, where);
	}
}
