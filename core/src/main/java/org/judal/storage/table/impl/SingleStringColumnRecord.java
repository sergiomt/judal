package org.judal.storage.table.impl;

import java.io.UnsupportedEncodingException;
import java.sql.Types;

import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;

import com.knowgate.stringutils.Html;
import org.judal.metadata.ViewDef;

public class SingleStringColumnRecord extends SingleObjectColumnRecord  {

	private static final long serialVersionUID = 1L;

	public SingleStringColumnRecord(ViewDef tableDef) {
		super(tableDef);
		setColumn(new ColumnDef(columnName, Types.VARCHAR, 1));
	}

	public SingleStringColumnRecord(String tableName) {
		super(tableName);
		setColumn(new ColumnDef(columnName, Types.VARCHAR, 1));
	}

	public SingleStringColumnRecord(String tableName, String columnName) {
		super(tableName, columnName);
		setColumn(new ColumnDef(columnName, Types.VARCHAR, 1));
	}

	@Override
	public boolean isEmpty(String colname) {
		return value == null || apply(colname).length()==0;
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
	public String getString(String colname) throws ClassCastException {
		return (String) value;
	}

	@Override
	public String getString(String colname, String defvalue) throws ClassCastException {
		return null==value ? defvalue : (String) value;
	}

	@Override
	public String getStringHtml(String colname, String defvalue) {
		String sStr = getString(colname, defvalue);
		return Html.encode(sStr);
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
