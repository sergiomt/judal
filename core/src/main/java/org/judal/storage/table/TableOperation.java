package org.judal.storage.table;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.Operation;
import org.judal.storage.Param;
import org.judal.storage.table.Record;

public class TableOperation<R extends Record> implements Operation {

	protected Table tbl;
	protected TableDataSource dts;
	private R rec;

	public TableOperation() {
		 this(EngineFactory.getDefaultTableDataSource());
	 }

	public TableOperation(R record) {
		 this(EngineFactory.getDefaultTableDataSource(), record);
	 }
	
	public TableOperation(TableDataSource dataSource) {
		dts = dataSource;
		rec = null;
	}

	public TableOperation(TableDataSource dataSource, R record) {
		dts = dataSource;
		rec = record;
		open();
	}

	@Override
	public TableDataSource dataSource() {
		return dts;
	}
	
	protected void open() {
		tbl = ((TableDataSource) dts).openTable(rec);
	}

	@Override
	public boolean exists(Object key) {
		return getTable().exists(key);
	}

	@Override
	public R load(Object key) throws JDOException, IllegalStateException {
		if (null == getRecord())
			throw new IllegalStateException("Record not set");
		return getTable().load(key, rec) ? rec : null;
	}

	@Override
	public void store() throws JDOException {
		if (null == getRecord())
			throw new IllegalStateException("Record not set");
		getTable().store(rec);
	}

	@Override
	public void delete(Object key) throws JDOException {
		getTable().delete(key);
	}

	public RecordSet<R> fetch(FetchGroup fetchGroup, String columnName, Object valueSearched) throws JDOException {
		return getTable().fetch(fetchGroup, columnName, valueSearched);
	}

	public void insert(Param... params) throws JDOException {
		getTable().insert(params);
	}

	public Table getTable() {
		return tbl;
	}

	@Override
	public void close() throws Exception {
		if (getTable() != null) {
			getTable().close();
			tbl = null;
		}
	}

	public R getRecord() {
		return rec;
	}

	public void setRecord(R record) throws JDOException {
		rec = record;
	}

}
