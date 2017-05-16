package org.judal.storage.table.impl;

import java.math.BigDecimal;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.ParseException;

import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;

public class SingleBigDecimalColumnRecord extends AbstractSingleNumberColumnRecord {

	private static final long serialVersionUID = 1L;

	public SingleBigDecimalColumnRecord(String tableName) {
		super(tableName);
	}

	public SingleBigDecimalColumnRecord(String tableName, String columnName) {
		super(tableName, columnName);
	}

	@Override
	public ColumnDef getColumn(String colname) throws ArrayIndexOutOfBoundsException {
		return new ColumnDef(columnName, Types.DECIMAL, 1);
	}

	@Override
	public BigDecimal apply(String colname) {
		return (BigDecimal) value;
	}

	@Override
	public BigDecimal getKey() throws JDOException {
		return (BigDecimal) super.getKey();		
	}

	@Override
	public BigDecimal getValue() throws JDOException {
		return (BigDecimal) super.getValue();
	}

	@Override
	public BigDecimal getDecimal(String colname) {
		if (isNull(colname))
			return null;
		else
			return apply(colname);
	}

	@Override
	public Short put(String colname, short ival) {
		Short retval = isNull(columnName) ? null : getValue().shortValue();
		columnName = colname;
		setValue(new BigDecimal(ival));
		return retval;
	}

	@Override
	public Integer put(String colname, int ival) {
		Integer retval = isNull(columnName) ? null : getValue().intValue();
		columnName = colname;
		setValue(new BigDecimal(ival));
		return retval;
	}

	@Override
	public Long put(String colname, long ival) {
		Long retval = isNull(columnName) ? null : getValue().longValue();
		columnName = colname;
		setValue(new BigDecimal(ival));
		return retval;
	}

	@Override
	public Float put(String colname, float fval) {
		Float retval = isNull(columnName) ? null : getValue().floatValue();
		columnName = colname;
		setValue(new BigDecimal(fval));
		return retval;
	}

	@Override
	public Double put(String colname, double dval) {
		Double retval = isNull(columnName) ? null : getValue().doubleValue();
		columnName = colname;
		setValue(new BigDecimal(dval));
		return retval;
	}

	@Override
	public BigDecimal put(String sColName, String sDecVal, DecimalFormat oPattern) throws ParseException {
		BigDecimal retval = getValue();
		put(sColName, oPattern.parse(sDecVal));
		return retval;
	}

	public BigDecimal put(String sColName, String sDecVal) throws ParseException {
		BigDecimal retval = getValue();
		put(sColName, new BigDecimal(sDecVal));
		return retval;
	}

}
