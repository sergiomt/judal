package org.judal.storage.table.impl;

import org.judal.metadata.ColumnDef;

public class SingleObjectColumnRecord extends AbstractSingleColumnRecord  {

	private static final long serialVersionUID = 1L;

	private ColumnDef columnDef;

	public SingleObjectColumnRecord(String tableName) {
		super(tableName);
	}

	public SingleObjectColumnRecord(String tableName, String columnName) {
		super(tableName, columnName);
	}

	public void setColumn(ColumnDef colDef) {
		columnDef = colDef;
	}

	@Override
	public ColumnDef getColumn(String colname) throws ArrayIndexOutOfBoundsException {
		return columnDef;
	}
	
	@Override
	public boolean isEmpty(String colname) {
		return value == null;
	}

	@Override
	public Object apply(String colname) {
		return value;
	}
	
}
