package org.judal.storage.java;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

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

}
