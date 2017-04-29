package org.judal.metadata.bind.test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import javax.jdo.JDOException;

import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.JDBCTableDataSource;
import org.judal.jdbc.test.TestJDBC;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.metadata.bind.JdoXmlMetadata;
import org.judal.storage.java.test.ArrayRecord1;
import org.judal.storage.table.TableDataSource;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestJdoXmlParsing {

	private static Map<String,String> properties;
	private static JDBCTableDataSource dts;
	
	private static final String dbName = "clocialdev";
	
	@BeforeClass
	public static void init() throws ClassNotFoundException, JDOException, IOException {
		properties = new TestJDBC().getTestProperties();
		JDBCEngine jdbc = new JDBCEngine();
		dts = jdbc.getDataSource(properties);
	}

	@AfterClass
	public static void clean() {
		dts.close();
	}
	
	@Ignore
	public void testSchemaParsing() throws NumberFormatException, ClassNotFoundException, NullPointerException, UnsatisfiedLinkError, SQLException, JDOException, IOException {
		TableDataSource ds = new JDBCTableDataSource(new TestJDBC().getTestProperties(),new JDBCEngine().getTransactionManager());

		JdoXmlMetadata xmlMeta = new JdoXmlMetadata(dts);
		SchemaMetaData metadata = xmlMeta.readMetadata(getClass().getResourceAsStream("jdometadata.xml"));

		assertEquals(2, metadata.tables().size());
		
		TableDef hardwired = ArrayRecord1.getTableDef(ds);
		TableDef xmlparsed = metadata.getTable(ArrayRecord1.tableName);
				
		assertEquals(hardwired.getName(), xmlparsed.getName());
		assertEquals(hardwired.getNumberOfColumns(), xmlparsed.getNumberOfColumns());
		assertEquals(hardwired.getPrimaryKeyMetadata().getColumn(), xmlparsed.getPrimaryKeyMetadata().getColumn());
	
		for (ColumnDef hcol: hardwired.getColumns()) {
			System.out.println("Checking column "+hardwired.getName()+"."+hcol.getName());
			ColumnDef xcol = xmlparsed.getColumnByName(hcol.getName());
			assertNotNull(xcol);
			assertEquals(hcol.getJDBCType(), xcol.getJDBCType());
			assertEquals(hcol.getLength(), xcol.getLength());
			assertEquals(hcol.getAllowsNull(), xcol.getAllowsNull());
		}
		ds.close();
	}
	
}