package org.judal.storage.table.impl;

import java.io.UnsupportedEncodingException;
import java.sql.Types;

import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;

public class SingleStringColumnRecord  extends AbstractSingleColumnRecord  {

	private static final long serialVersionUID = 1L;

	public SingleStringColumnRecord(String tableName) {
		super(tableName);
	}

	public SingleStringColumnRecord(String tableName, String columnName) {
		super(tableName, columnName);
	}

	@Override
	public ColumnDef getColumn(String colname) throws ArrayIndexOutOfBoundsException {
		return new ColumnDef(columnName, Types.VARCHAR, 1);
	}
	
	@Override
	public boolean isEmpty(String colname) {
		return value == null;
	}

	@Override
	public String apply(String colname) {
		return (String) value;
	}

	@Override
	public String getKey() throws JDOException {
		return (String) super.getKey();		
	}

	@Override
	public String getValue() throws JDOException {
		return (String) super.getValue();
	}

	@Override
	public void setContent(byte[] bytes, String characterEncoding) throws JDOException {
		try {
			setValue(new String(bytes, characterEncoding));
		} catch (UnsupportedEncodingException e) {
			throw new JDOException("UnsupportedEncodingException "+characterEncoding, e);
		}
	}

	@Override
	public String put(String colname, Object obj) throws IllegalArgumentException {
		String retval = isNull(colname) ? null : getValue();
		if (obj==null)
			value = null;
		else if (obj instanceof String)
			value = (String) obj;
		else
			value = obj.toString();
		return retval;
	}
	
}
