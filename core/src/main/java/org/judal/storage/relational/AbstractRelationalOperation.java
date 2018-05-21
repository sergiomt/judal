package org.judal.storage.relational;

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

import java.util.Date;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.query.Predicate;
import org.judal.storage.table.AbstractIndexableTableOperation;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.View;

import com.knowgate.dateutils.DateHelper;

import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.relational.RelationalTable;

/**
 * <p>Base class for relational operation wrapper.</p>
 * Be sure to always call close() after an operation is done or else a connection leak may occur.
 * @author Sergio Montoro Ten
 * @param &lt;R extends Record&gt;
 * @version 1.0
 */
public abstract class AbstractRelationalOperation<R extends Record> extends AbstractIndexableTableOperation<R> {

	/**
	 * <p>Constructor.</p>
	 * Create RelationalOperation using EngineFactory default relational data source.
	 */
	public AbstractRelationalOperation() {
		this(EngineFactory.getDefaultRelationalDataSource());
	}

	/**
	 * <p>Constructor.</p>
	 * Create RelationalOperation using EngineFactory default relational data source.
	 * @param record R Instance of Record subclass to be used by this operation.
	 */
	public AbstractRelationalOperation(R record) {
		 this(EngineFactory.getDefaultRelationalDataSource(), record);
	 }

	/**
	 * <p>Constructor.</p>
	 * Create RelationalOperation using given relational data source.
	 * @param dataSource RelationalDataSource
	 */
	public AbstractRelationalOperation(RelationalDataSource dataSource) {
		super(dataSource);
	}

	/**
	 * <p>Constructor.</p>
	 * Create RelationalOperation using given relational data source.
	 * @param dataSource RelationalDataSource
	 * @param record R Instance of Record subclass to be used by this operation.
	 */
	public AbstractRelationalOperation(RelationalDataSource dataSource, R record) {
		super(dataSource, record);
	}

	@Override
	protected void open() {
		tbl = dataSource().openRelationalTable(getRecord());
	}

	/**
	 * @return RelationalDataSource
	 */
	public RelationalDataSource dataSource() {
		return (RelationalDataSource) dts;
	}
	
	/**
	 * @return RelationalTable
	 */
	public RelationalTable getTable() {
		return (RelationalTable) tbl;
	}

	/**
	 * <p>Count results returned by a filter predicate.</p>
	 * @param filterPredicate Predicate
	 * @return long
	 * @throws JDOException
	 */
	public long count(Predicate filterPredicate) throws JDOException {
		return getTable().count(filterPredicate);
	}

	/**
	 * <p>Get count of records with a given value within a range in the specified column.</p>
	 * @param columnName String Column Name
	 * @param lowerBound Object Lower bound inclusive
	 * @param upperBound Object Upper bound inclusive
	 * @return long Count of rows which value for the given column is within the specified range
	 * @throws JDOException
	 */
	public long count(String columnName, Object lowerBound, Object upperBound) throws JDOException {
		RecordSet<Record> oRst = getTable().fetch(new ColumnGroup("COUNT(*) AS " + View.NUM_ROWS), columnName, lowerBound, upperBound, 1, 0);
		return oRst.get(0).getLong(View.NUM_ROWS);
	}

	/**
	 * <p>Sum results returned by a filter predicate in the given column.</p>
	 * @param columnName String
	 * @param filterPredicate Predicate
	 * @return Number
	 * @throws JDOException
	 */
	public Number sum(String columnName, Predicate filterPredicate) throws JDOException {
		return getTable().sum(columnName, filterPredicate);
	}

	/**
	 * <p>Average results returned by a filter predicate in the given column.</p>
	 * @param columnName String
	 * @param filterPredicate Predicate
	 * @return Number
	 * @throws JDOException
	 */
	public Number avg(String columnName, Predicate filterPredicate) throws JDOException {
		return getTable().avg(columnName, filterPredicate);
	}

	/**
	 * <p>Get maximum Date found in the given column for the results returned by a filter predicate.</p>
	 * @param columnName String
	 * @param filterPredicate Predicate
	 * @return Date or <b>null</b>
	 * @throws JDOException
	 */
	public Date maxDate(String columnName, Predicate filterPredicate) throws JDOException {
		return DateHelper.toDate(getTable().max(columnName, filterPredicate));
	}
	
	/**
	 * <p>Get minimum Date found in the given column for the results returned by a filter predicate.</p>
	 * @param columnName String
	 * @param filterPredicate Predicate
	 * @return Date or <b>null</b>
	 * @throws JDOException
	 */
	public Date minDate(String columnName, Predicate filterPredicate) throws JDOException {
		return DateHelper.toDate(getTable().min(columnName, filterPredicate));
	}

	/**
	 * <p>Get maximum Integer found in the given column for the results returned by a filter predicate.</p>
	 * @param columnName String
	 * @param filterPredicate Predicate
	 * @return Integer or <b>null</b>
	 * @throws JDOException
	 */
	public Integer maxInt(String columnName, Predicate filterPredicate) throws JDOException {
		return (Integer) getTable().max(columnName, filterPredicate);
	}
	
	/**
	 * <p>Get minimum Integer found in the given column for the results returned by a filter predicate.</p>
	 * @param columnName String
	 * @param filterPredicate Predicate
	 * @return Integer or <b>null</b>
	 * @throws JDOException
	 */
	public Integer minInteger(String columnName, Predicate filterPredicate) throws JDOException {
		return (Integer) getTable().min(columnName, filterPredicate);
	}

	/**
	 * <p>Get maximum Integer found in the given column for the results returned by a filter predicate.</p>
	 * @param columnName String
	 * @param filterPredicate Predicate
	 * @return Long or <b>null</b>
	 * @throws JDOException
	 */
	public Long maxLong(String columnName, Predicate filterPredicate) throws JDOException {
		return (Long) getTable().max(columnName, filterPredicate);
	}
	
	/**
	 * <p>Get minimum Integer found in the given column for the results returned by a filter predicate.</p>
	 * @param columnName String
	 * @param filterPredicate Predicate
	 * @return Long or <b>null</b>
	 * @throws JDOException
	 */
	public Long minLong(String columnName, Predicate filterPredicate) throws JDOException {
		return (Long) getTable().min(columnName, filterPredicate);
	}

	/**
	 * <p>Create new Predicate.</p>
	 * @return Predicate
	 * @throws JDOException
	 */
	public Predicate newPredicate() throws JDOException {
		return getTable().newPredicate();
	}

}
