package org.judal.storage.table.impl;

import java.math.BigDecimal;
import java.sql.Types;

import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;

public class SingleIntColumnRecord extends AbstractSingleNumberColumnRecord {

	private static final long serialVersionUID = 1L;

	public SingleIntColumnRecord(String tableName) {
		super(tableName);
	}

	public SingleIntColumnRecord(String tableName, String columnName) {
		super(tableName, columnName);
	}

	@Override
	public ColumnDef getColumn(String colname) throws ArrayIndexOutOfBoundsException {
		return new ColumnDef(columnName, Types.INTEGER, 1);
	}

	@Override
	public Integer apply(String colname) {
		return (Integer) value;
	}

	@Override
	public Integer getKey() throws JDOException {
		return (Integer) super.getKey();		
	}

	@Override
	public Integer getValue() throws JDOException {
		return (Integer) super.getValue();
	}
	
	@Override
	public BigDecimal getDecimal(String colname) {
		if (isNull(colname))
			return null;
		else
			return new BigDecimal(apply(colname));
	}

	@Override
	public Short put(String colname, short ival) {
		Short retval = isNull(columnName) ? null : getValue().shortValue();
		columnName = colname;
		setValue(new Integer(ival));
		return retval;
	}

	@Override
	public Integer put(String colname, int ival) {
		Integer retval = isNull(columnName) ? null : getValue();
		columnName = colname;
		setValue(ival);
		return retval;
	}

	@Override
	public Long put(String colname, long ival) {
		Long retval = isNull(columnName) ? null : getValue().longValue();
		columnName = colname;
		setValue(new Integer((int) ival));
		return retval;
	}
	
}
