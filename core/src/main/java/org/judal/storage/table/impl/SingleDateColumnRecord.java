package org.judal.storage.table.impl;

import java.sql.Timestamp;

import java.io.UnsupportedEncodingException;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;

import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;

public class SingleDateColumnRecord  extends SingleObjectColumnRecord  {

	private static final long serialVersionUID = 1L;

	public SingleDateColumnRecord(String tableName) {
		super(tableName);
	}

	public SingleDateColumnRecord(String tableName, String columnName) {
		super(tableName, columnName);
		setColumn(new ColumnDef(columnName, Types.DATE, 1));
	}

	@Override
	public boolean isEmpty(String colname) {
		return value == null;
	}

	@Override
	public Date apply(String colname) {
		return getValue();
	}

	@Override
	public Date getKey() throws JDOException {
		return getValue();	
	}

	@Override
	public Date getValue() throws JDOException {
		if (null==value) {
			return null;
		} else if (value instanceof Timestamp) {
			return new Date(((Timestamp) value).getTime());
		} else if (value instanceof java.sql.Date) {
			java.sql.Date d = (java.sql.Date) value; 
			return new Date(d.getYear(), d.getMonth(), d.getDate());
		} else if (value instanceof Calendar) {
			return new Date(((Calendar) value).getTimeInMillis());			
		} else
			throw new ClassCastException("Cannot cast from "+value.getClass().getName()+" to java.util.Date");
	}
	
}
