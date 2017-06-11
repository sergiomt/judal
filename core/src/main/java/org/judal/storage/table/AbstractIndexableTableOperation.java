package org.judal.storage.table;


import org.judal.storage.EngineFactory;
import org.judal.storage.Param;
import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;

public abstract class AbstractIndexableTableOperation<R extends Record> extends AbstractTableOperation<R> {

	public AbstractIndexableTableOperation() {
		this(EngineFactory.getDefaultTableDataSource());
	}

	public AbstractIndexableTableOperation(R record) {
		this(EngineFactory.getDefaultTableDataSource(), record);
	}

	public AbstractIndexableTableOperation(TableDataSource dataSource) {
		super(dataSource);
	}

	public AbstractIndexableTableOperation(TableDataSource dataSource, R record) {
		super(dataSource, record);
	}
	
	protected void open() {
		tbl = ((TableDataSource) dts).openIndexedTable(getRecord());
	}

	@Override
	public IndexableTable getTable() {
		return (IndexableTable) tbl;
	}

	public long count(String columnName, Object valueSearched) {
		return getTable().count(columnName, valueSearched);
	}

	public boolean exists(Param... keys) {
		return getTable().exists(keys);
	}

	public abstract Object fetch(final int maxrows, final int offset, Param... keys);
	
	public int update(Param[] values, Param[] where) {
		return getTable().update(values, where);
	}
}
