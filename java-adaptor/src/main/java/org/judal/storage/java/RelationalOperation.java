package org.judal.storage.java;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.judal.storage.Param;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

import org.judal.storage.relational.AbstractRelationalOperation;
import org.judal.storage.relational.RelationalDataSource;

public class RelationalOperation<R extends Record> extends AbstractRelationalOperation<R> {

	public RelationalOperation() {
	}

	public RelationalOperation(R record) {
		 super(record);
	 }

	public RelationalOperation(RelationalDataSource dataSource) {
		super(dataSource);
	}

	public RelationalOperation(RelationalDataSource dataSource, R record) {
		super(dataSource, record);
	}

	@Override
	public RecordSet<R> fetch(final int maxrows, final int offset, Param... keys) {
		return getTable().fetch(getRecord().fetchGroup(), maxrows, offset, keys);
	}

	@Override
	public RecordSet<R> fetch(FetchGroup fetchGroup, String columnName, Object valueSearched) throws JDOException {
		return getTable().fetch(fetchGroup, columnName, valueSearched);
	}

	@Override
	public Object fetchAsc(FetchGroup fetchGroup, String columnName, Object valueSearched, String sortByColumn)
			throws JDOException {
		RecordSet<R> retval = getTable().fetch(fetchGroup, columnName, valueSearched);
		retval.sort(sortByColumn);
		return retval;
	}

	@Override
	public Object fetchDesc(FetchGroup fetchGroup, String columnName, Object valueSearched, String sortByColumn)
			throws JDOException {
		RecordSet<R> retval = getTable().fetch(fetchGroup, columnName, valueSearched);
		retval.sort(sortByColumn);
		return retval;
	}

}
