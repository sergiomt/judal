package org.judal.storage.java.test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

import javax.jdo.JDOException;

import org.judal.metadata.TableDef;
import org.judal.metadata.IndexDef.Type;
import org.judal.storage.java.ArrayRecord;
import org.judal.storage.table.TableDataSource;

public class ArrayRecord1 extends ArrayRecord implements TestRecord1 {

	public static String tableName = "unittest_table1";
	
	public static TableDataSource dataSource = null;

	public ArrayRecord1() throws JDOException {
		super(getTableDef(dataSource));
	}

	public static TableDef getTableDef(TableDataSource ds) throws JDOException {
		TableDef tbl;
		if (null==ds)
			tbl = new TableDef(tableName);
		else
			tbl = ds.createTableDef(tableName, null);
		tbl.setRecordClass(ArrayRecord1.class);
		tbl.addPrimaryKeyColumn("default", "id", Types.INTEGER);
		tbl.addColumnMetadata("default", "created", Types.TIMESTAMP, false);
		tbl.addColumnMetadata("default", "name", Types.VARCHAR, 100, false, Type.MANY_TO_ONE);
		tbl.addColumnMetadata("default", "description", Types.VARCHAR, 255, true);
		tbl.addColumnMetadata("default", "location", Types.VARCHAR, 255, true);
		tbl.addColumnMetadata("default", "image", Types.LONGVARBINARY, true);
		tbl.addColumnMetadata("default", "amount", Types.NUMERIC, 8, 2, true, Type.MANY_TO_ONE, null, null);
		return tbl;
	}

	@Override
	public TableDef getTableDef() {
		return ArrayRecord1.getTableDef(dataSource);
	}

	public Integer getId() { return getInteger("id"); }

	public void setId(Integer n) { put("id", n); }

	public Date getCreated() { return getDate("created"); }

	public void setCreated(long timeMilis) { put("created", new Timestamp(System.currentTimeMillis())); }

	public String getName() { return getString("name"); }

	public void setName(String n) { put("name", n); }

	public String getLocation() { return getString("location"); }

	public void setLocation(String n) { put("location", n); }

	public byte[] getImage() { return getBytes("image"); }

	public void setImage(byte[] bytes) { put("image", bytes); }

	public BigDecimal getAmount() { return getDecimal("amount"); }

	public void setAmount(BigDecimal n) { put("amount", n); }	
	
}
