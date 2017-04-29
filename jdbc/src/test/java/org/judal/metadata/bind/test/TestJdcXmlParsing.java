package org.judal.metadata.bind.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import javax.jdo.JDOException;

import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.JDBCTableDataSource;
import org.judal.jdbc.metadata.SQLXmlMetadata;
import org.judal.jdbc.test.TestJDBC;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.storage.java.test.ArrayRecord1;
import org.judal.storage.table.TableDataSource;

public class TestJdcXmlParsing {

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
	
	@Test
	public void testSchemaParsing() throws NumberFormatException, ClassNotFoundException, NullPointerException, UnsatisfiedLinkError, SQLException, JDOException, IOException {
		TableDataSource ds = new JDBCTableDataSource(new TestJDBC().getTestProperties(),new JDBCEngine().getTransactionManager());

		SQLXmlMetadata xmlMeta = new SQLXmlMetadata(dts);		
		SchemaMetaData metadata = xmlMeta.readMetadata(xmlMeta.openStream("org/judal/metadata/bind/test", "ddlmetadata.xml"));
		
		for (TableDef tdef : metadata.tables()) {
			System.out.println(tdef.getName());
		}
		ds.close();
	}
	
}
