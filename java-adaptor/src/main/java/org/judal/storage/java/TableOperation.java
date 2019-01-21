package org.judal.storage.java;

import static org.judal.storage.query.SortDirection.ASC;
import static org.judal.storage.query.SortDirection.DESC;
import static org.judal.storage.query.SortDirection.same;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;
import javax.jdo.JDOUserException;

import org.judal.storage.table.AbstractTableOperation;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.TableDataSource;

public class TableOperation<R extends Record> extends AbstractTableOperation<R> {

	public TableOperation() {
	}

	public TableOperation(R record) {
		 super(record);
	 }
	
	public TableOperation(TableDataSource dataSource) {
		super(dataSource);
	}

	public TableOperation(TableDataSource dataSource, R record) {
		super(dataSource, record);
	}
	
	public RecordSet<R> fetch(FetchGroup fetchGroup, String columnName, Object valueSearched) throws JDOException {
		return getTable().fetch(fetchGroup, columnName, valueSearched);
	}

	public RecordSet<R> fetchAsc(FetchGroup fetchGroup, String columnName, Object valueSearched, String sortByColumn) throws JDOException {
		RecordSet<R> retval = getTable().fetch(fetchGroup, columnName, valueSearched);
		retval.sort(sortByColumn);
		return retval;
	}

	public RecordSet<R> fetchDesc(FetchGroup fetchGroup, String columnName, Object valueSearched, String sortByColumn) throws JDOException {
		RecordSet<R> retval = getTable().fetch(fetchGroup, columnName, valueSearched);
		retval.sortDesc(sortByColumn);
		return retval;
	}

	@Override
	public R fetchFirst(FetchGroup fetchGroup, String columnName, Object valueSearched, String... sortBy)
		throws JDOException {
		RecordSet<R> rst;
		if (sortBy==null || sortBy.length==0)
			rst = fetch(fetchGroup, columnName, valueSearched);
		else if (sortBy.length==1)
			rst = fetchAsc(fetchGroup, columnName, valueSearched, sortBy[0]);
		else
			if (same(ASC,sortBy[1]))
				rst = fetchAsc(fetchGroup, columnName, valueSearched, sortBy[0]);
			else if (same(DESC,sortBy[1]))
				rst = fetchDesc(fetchGroup, columnName, valueSearched, sortBy[0]);
			else
				throw new JDOUserException("Unrecognized sort direction " + sortBy[1]);
		setRecord(rst.size()>0 ? rst.get(0) : null);
		return getRecord();
	}

}
