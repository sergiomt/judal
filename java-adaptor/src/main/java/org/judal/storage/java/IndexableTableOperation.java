package org.judal.storage.java;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.judal.storage.Param;
import org.judal.storage.table.AbstractIndexableTableOperation;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.TableDataSource;

public class IndexableTableOperation<R extends Record> extends AbstractIndexableTableOperation<R> {

	public IndexableTableOperation() {
	}

	public IndexableTableOperation(R record) {
		super(record);
	}

	public IndexableTableOperation(TableDataSource dataSource) {
		super(dataSource);
	}

	public IndexableTableOperation(TableDataSource dataSource, R record) {
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
	public RecordSet<R> fetchAsc(FetchGroup fetchGroup, String columnName, Object valueSearched, String sortByColumn)
			throws JDOException {
		RecordSet<R> retval = fetch(fetchGroup, columnName, valueSearched);
		retval.sort(sortByColumn);
		return retval;
	}

	@Override
	public RecordSet<R> fetchDesc(FetchGroup fetchGroup, String columnName, Object valueSearched, String sortByColumn)
			throws JDOException {
		RecordSet<R> retval = fetch(fetchGroup, columnName, valueSearched);
		retval.sortDesc(sortByColumn);
		return retval;
	}

}
