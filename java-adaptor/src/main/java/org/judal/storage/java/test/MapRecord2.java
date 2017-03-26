package org.judal.storage.java.test;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

import javax.jdo.JDOException;

import org.judal.metadata.TableDef;
import org.judal.metadata.IndexDef.Type;
import org.judal.storage.TableDataSource;
import org.judal.storage.java.MapRecord;

public class MapRecord2 extends MapRecord implements TestRecord2 {

	public static String tableName = "unittest_table2";
	
	public static TableDataSource dataSource = null;

	public MapRecord2() throws JDOException {
		super(getTableDef(dataSource));
	}

	public static TableDef getTableDef(TableDataSource ds) throws JDOException {
		TableDef tbl;
		if (null==ds)
			tbl = new TableDef(tableName);
		else
			tbl = ds.createTableDef(tableName, null);
		tbl.setRecordClass(MapRecord2.class);
		tbl.addPrimaryKeyColumn(null, "code", Types.VARCHAR, 100);
		tbl.addColumnMetadata("default", "name", Types.VARCHAR, 100, false, Type.ONE_TO_ONE);
		tbl.addColumnMetadata("default", "created", Types.TIMESTAMP, false);
		tbl.addColumnMetadata("default", "description", Types.VARCHAR, 255, true);
		return tbl;
	}

	@Override
	public Date getCreated() {
		return getDate("created");
	}

	@Override
	public void setCreated(long timeMilis) {
		put("created", new Timestamp(timeMilis));
	}

	@Override
	public String getCode() {
		return getString("code");
	}

	@Override
	public void setCode(String c) {
		put ("code",c);
	}
	
	@Override
	public String getName() {
		return getString("name");
	}

	@Override
	public void setName(String n) {
		put ("name",n);
	}

	@Override
	public String getDescription() {
		return getString("description");
	}

	@Override
	public void setDescription(String d) {
		put ("description",d);
	}
	
}
