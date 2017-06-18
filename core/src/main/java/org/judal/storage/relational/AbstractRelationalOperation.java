package org.judal.storage.relational;


import java.util.Date;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.query.Predicate;
import org.judal.storage.table.AbstractIndexableTableOperation;
import org.judal.storage.table.Record;

import com.knowgate.dateutils.DateHelper;

import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.relational.RelationalTable;

public abstract class AbstractRelationalOperation<R extends Record> extends AbstractIndexableTableOperation<R> {

	public AbstractRelationalOperation() {
		this(EngineFactory.getDefaultRelationalDataSource());
	}

	public AbstractRelationalOperation(R record) {
		 this(EngineFactory.getDefaultRelationalDataSource(), record);
	 }

	public AbstractRelationalOperation(RelationalDataSource dataSource) {
		super(dataSource);
	}

	public AbstractRelationalOperation(RelationalDataSource dataSource, R record) {
		super(dataSource, record);
	}

	protected void open() {
		tbl = dataSource().openRelationalTable(getRecord());
	}

	public RelationalDataSource dataSource() {
		return (RelationalDataSource) dts;
	}
	
	public RelationalTable getTable() {
		return (RelationalTable) tbl;
	}

	public long count(Predicate filterPredicate) throws JDOException {
		return getTable().count(filterPredicate);
	}

	public Number sum(String columnName, Predicate filterPredicate) throws JDOException {
		return getTable().sum(columnName, filterPredicate);
	}

	public Number avg(String columnName, Predicate filterPredicate) throws JDOException {
		return getTable().avg(columnName, filterPredicate);
	}

	public Date maxDate(String columnName, Predicate filterPredicate) throws JDOException {
		return DateHelper.toDate(getTable().max(columnName, filterPredicate));
	}
	
	public Date minDate(String columnName, Predicate filterPredicate) throws JDOException {
		return DateHelper.toDate(getTable().min(columnName, filterPredicate));
	}

	public Integer maxInt(String columnName, Predicate filterPredicate) throws JDOException {
		return (Integer) getTable().max(columnName, filterPredicate);
	}
	
	public Integer minInteger(String columnName, Predicate filterPredicate) throws JDOException {
		return (Integer) getTable().min(columnName, filterPredicate);
	}

	public Long maxLong(String columnName, Predicate filterPredicate) throws JDOException {
		return (Long) getTable().max(columnName, filterPredicate);
	}
	
	public Long minLong(String columnName, Predicate filterPredicate) throws JDOException {
		return (Long) getTable().min(columnName, filterPredicate);
	}

	public Predicate newPredicate() throws JDOException {
		return getTable().newPredicate();
	}

}
