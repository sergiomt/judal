package org.judal.jdbc.test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import javax.jdo.JDOException;

import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.metadata.SQLFunctions;
import org.judal.metadata.TableDef;
import org.judal.storage.java.MapRecord;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Connective;
import org.judal.storage.query.Operator;
import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.relational.RelationalTable;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.impl.SingleLongColumnRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestSQLFunctions extends TestJDBC {

	private static Map<String,String> properties;
	private static RelationalDataSource dts;

	@BeforeClass
	public static void init() throws ClassNotFoundException, JDOException, IOException {
		properties = new TestJDBC().getTestProperties();
		JDBCEngine jdbc = new JDBCEngine();
		dts = jdbc.getDataSource(properties);
	}

	@AfterClass
	public static void destroy() {
		dts.close();
	}
	
	@Test
	public void test01() throws JDOException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		HashMap<String,Object> tableProperties = new HashMap<>();
		TableDef tdef = dts.createTableDef("test_sql_funcs", tableProperties);
		tdef.addPrimaryKeyColumn("", "pk", Types.INTEGER);
		tdef.addColumnMetadata("", "dt", Types.TIMESTAMP, 8, true);
		tdef.addColumnMetadata("", "name", Types.VARCHAR, 100, true);
		tdef.addColumnMetadata("", "desc", Types.VARCHAR, 2000, true);
		dts.createTable(tdef, tableProperties);
		
		MapRecord rec = new MapRecord(tdef);

		SQLFunctions fn = new SQLFunctions(dts.getRdbmsId());
		
		try (IndexableTable tbl = dts.openIndexedTable(rec)) {
			rec.put("pk", 1);
			rec.put("dt", new Timestamp(System.currentTimeMillis()));
			rec.put("name", "Name of #1");
			rec.put("desc", "Description of #1");
			tbl.store(rec);
			rec.clear();
			rec.put("pk", 2);
			rec.put("dt", new Timestamp(System.currentTimeMillis()));
			rec.put("name", "The Name of #1");
			rec.put("desc", "Description of #2");
			tbl.store(rec);
			rec.clear();
			rec.put("pk", 3);
			rec.put("dt", new Timestamp(System.currentTimeMillis()));
			rec.put("name", "The Name of #1");
			tbl.store(rec);
			rec.clear();
			rec.put("pk", 4);
			rec.put("dt", new Timestamp(System.currentTimeMillis()));
			rec.put("name", "The Name of #1");
			tbl.store(rec);
			rec.clear();

			RecordSet<MapRecord> rst = tbl.fetch(new ColumnGroup(fn.LENGTH+"(name) AS NameLength"), "pk", new Integer(1));
			assertEquals(1, rst.size());
			assertEquals(10, rst.get(0).getInt("NameLength"));			
		}
		
		SingleLongColumnRecord sir = new SingleLongColumnRecord("test_sql_funcs", "RecCount");
		try (RelationalTable tbl = dts.openRelationalTable(sir)) {
			AbstractQuery qry = (AbstractQuery) tbl.newQuery();
			qry.setResult("COUNT(*) AS RecCount");
			qry.setResultClass(SingleLongColumnRecord.class);
			qry.setFilter(qry.newPredicate(Connective.AND).add("desc", Operator.ISNULL));
			RecordSet<SingleLongColumnRecord> ast = tbl.fetch(qry);
			assertEquals(2, ast.get(0).getValue().intValue());
			qry.setFilter(qry.newPredicate(Connective.AND).add(fn.ISNULL+"(desc,'Description of #1')", Operator.EQ, "Description of #1"));
			ast = tbl.fetch(qry);
			assertEquals(3, ast.get(0).getValue().intValue());
		}

		dts.dropTable("test_sql_funcs", false);
	}
	
}
