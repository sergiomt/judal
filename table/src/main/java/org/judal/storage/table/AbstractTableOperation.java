package org.judal.storage.table;

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

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.Operation;
import org.judal.storage.Param;
import org.judal.storage.table.Record;
import org.judal.storage.table.Table;
import org.judal.storage.table.TableDataSource;

/**
 * <p>Base class for table operation wrapper.</p>
 * Be sure to always call close() after an operation is done or else a connection leak may occur.
 * @author Sergio Montoro Ten
 * @param &lt;R extends Record&gt;
 * @version 1.0
 */
public abstract class AbstractTableOperation<R extends Record> implements Operation {

	protected Table tbl;
	protected TableDataSource dts;
	private R rec;

	/**
	 * <p>Constructor.</p>
	 * Create TableOperation using EngineFactory default table data source or default relational data source if default table data source is null.
	 */
	protected AbstractTableOperation() throws NullPointerException {
		 this(EngineFactory.getDefaultTableDataSource()==null ? EngineFactory.getDefaultRelationalDataSource() : EngineFactory.getDefaultTableDataSource());
	 }

	/**
	 * <p>Constructor.</p>
	 * Create IndexableTableOperation using EngineFactory default table data source or default relational data source if default table data source is null.
	 * @param record R Instance of Record subclass to be used by this operation.
	 * @throws NullPointerException if record is null
	 */
	protected AbstractTableOperation(R record) throws NullPointerException {
		 this(EngineFactory.getDefaultTableDataSource()==null ? EngineFactory.getDefaultRelationalDataSource() : EngineFactory.getDefaultTableDataSource(), record);
	 }
	
	/**
	 * <p>Constructor.</p>
	 * Create TableOperation using given table data source.
	 * @param dataSource TableDataSource
	 * @throws NullPointerException if dataSource is null
	 */
	protected AbstractTableOperation(TableDataSource dataSource) throws NullPointerException {
		if (null==dataSource)
			throw new NullPointerException("AbstractTableOperation constructor. TableDataSource cannot be null");
		dts = dataSource;
		rec = null;
	}

	/**
	 * <p>Constructor.</p>
	 * Create TableOperation using given table data source.
	 * @param dataSource TableDataSource
	 * @param record R Instance of Record subclass to be used by this operation.
	 * @throws NullPointerException if dataSource is null or record is null
	 */
	protected AbstractTableOperation(TableDataSource dataSource, R record) throws NullPointerException {
		if (null==dataSource)
			throw new NullPointerException("AbstractTableOperation constructor. TableDataSource cannot be null");
		if (null==record)
			throw new NullPointerException("AbstractTableOperation constructor. Record cannot be null");
		dts = dataSource;
		rec = record;
		open();
	}

	/**
	 * @return TableDataSource
	 */
	@Override
	public TableDataSource dataSource() {
		return dts;
	}

	protected void open() {
		tbl = ((TableDataSource) dts).openTable(rec);
	}

	/**
	 * <p>Check if Record with the given value for  its primary key exists.</p>
	 * @return boolean
	 */
	@Override
	public boolean exists(Object key) {
		return getTable().exists(key);
	}

	/**
	 * <p>Load Record by primary key.</p>
	 * @param key Object
	 * @return R
	 * @throws JDOException
	 * @throws IllegalStateException If no Record has been set for this operation.
	 */
	@Override
	public R load(Object key) throws JDOException, IllegalStateException {
		if (null == getRecord())
			throw new IllegalStateException("Record not set");
		return getTable().load(key, rec) ? rec : null;
	}

	/**
	 * <p>Store Record.</p>
	 * @throws JDOException
	 * @throws IllegalStateException If no Record has been set for this operation.
	 */
	@Override
	public void store() throws JDOException {
		if (null == getRecord())
			throw new IllegalStateException("Record not set");
		getTable().store(rec);
	}

	/**
	 * <p>Delete Record by primary key.</p>
	 * @param key Object
	 * @throws JDOException
	 */
	@Override
	public void delete(Object key) throws JDOException {
		getTable().delete(key);
	}

	/**
	 * <p>Fetch records which contain a certain value at a given column.</p>
	 * @param fetchGroup FetchGroup Columns to be fetched
	 * @param columnName String Column name
	 * @param valueSearched Object Value that must be present at column columnName
	 * @return Object an Iterable which type depends on whether the implementation is done by the Java or by the Scala adaptor.
	 * @throws JDOException
	 */
	public abstract Object fetch(FetchGroup fetchGroup, String columnName, Object valueSearched) throws JDOException;

	/**
	 * <p>Fetch records which contain a certain value at a given column.</p>
	 * @param fetchGroup FetchGroup Columns to be fetched
	 * @param columnName String Column name
	 * @param valueSearched Object Value that must be present at column columnName
	 * @param sortBy Optional. String column and direction (ASC or DESC) used to sort the values. Must be present in the FetchGroup
	 * @return R extends Record.
	 * @throws JDOException
	 */
	public abstract R fetchFirst(FetchGroup fetchGroup, String columnName, Object valueSearched, String... sortBy) throws JDOException;

	/**
	 * <p>Fetch records which contain a certain value at a given column and return results sorted in ascending order.</p>
	 * @param fetchGroup FetchGroup Columns to be fetched
	 * @param columnName String Column name
	 * @param valueSearched Object Value that must be present at column columnName
	 * @param sortByColum String Column used to sort the values. Must be present in the FetchGroup
	 * @return Object an Iterable which type depends on whether the implementation is done by the Java or by the Scala adaptor.
	 * @throws JDOException
	 */
	public abstract Object fetchAsc(FetchGroup fetchGroup, String columnName, Object valueSearched, String sortByColum) throws JDOException;

	/**
	 * <p>Fetch records which contain a certain value at a given column and return results sorted in descending order.</p>
	 * @param fetchGroup FetchGroup Columns to be fetched
	 * @param columnName String Column name
	 * @param valueSearched Object Value that must be present at column columnName
	 * @param sortByColum String Column used to sort the values. Must be present in the FetchGroup
	 * @return Object an Iterable which type depends on whether the implementation is done by the Java or by the Scala adaptor.
	 * @throws JDOException
	 */
	public abstract Object fetchDesc(FetchGroup fetchGroup, String columnName, Object valueSearched, String sortByColum) throws JDOException;

	/**
	 * <p>Insert a new Record.</p>
	 * @param params Param&hellip;
	 * @throws JDOException If another Record with the same primary key already exists.
	 */
	public void insert(Param... params) throws JDOException {
		getTable().insert(params);
	}

	/**
	 * @return Table
	 */
	public Table getTable() {
		return tbl;
	}

	@Override
	public void close() {
		if (getTable() != null) {
			getTable().close();
			tbl = null;
		}
	}

	/**
	 * @return FetchGroup
	 * @throws IllegalStateException If no Record has been set for this operation.
	 */
	public FetchGroup fetchGroup() throws IllegalStateException {
		if (null == getRecord())
			throw new IllegalStateException("Record not set");
		return getRecord().fetchGroup();
	}

	/**
	 * @return R
	 */
	public R getRecord() {
		return rec;
	}

	/**
	 * @param record R
	 * @throws JDOException
	 */
	public void setRecord(R record) throws JDOException {
		rec = record;
	}

}
