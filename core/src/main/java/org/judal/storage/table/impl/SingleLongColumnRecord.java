package org.judal.storage.table.impl;

import java.math.BigDecimal;
import java.sql.Types;

import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;

public class SingleLongColumnRecord extends AbstractSingleNumberColumnRecord {

	private static final long serialVersionUID = 1L;

	public SingleLongColumnRecord(String tableName) {
		super(tableName);
		setColumn(new ColumnDef("value", Types.BIGINT, 1));
	}

	public SingleLongColumnRecord(String tableName, String columnName) {
		super(tableName, columnName);
		setColumn(new ColumnDef(columnName, Types.BIGINT, 1));
	}

	@Override
	public Long apply(String colname) {
		return (Long) value;
	}

	@Override
	public Long getKey() throws JDOException {
		return (Long) super.getKey();		
	}

	@Override
	public Long getValue() throws JDOException {
		return (Long) super.getValue();
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
		setValue(new Long(ival));
		return retval;
	}

	@Override
	public Integer put(String colname, int ival) {
		Integer retval = isNull(columnName) ? null : getValue().intValue();
		columnName = colname;
		setValue(new Long(ival));
		return retval;
	}

	@Override
	public Long put(String colname, long ival) {
		Long retval = isNull(columnName) ? null : getValue();
		columnName = colname;
		setValue(ival);
		return retval;
	}
	
}
