package org.judal.metadata.bind.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import java.util.Map;

import javax.jdo.JDOException;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.io.DatabaseIO;

import org.junit.Test;
import org.junit.Ignore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.bind.JdoXmlMetadata;

import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.JDBCTableDataSource;
import org.judal.jdbc.test.TestJDBC;

public class TestGenerateXmlSchema {

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
	public void test01GenerateXmlSchema() throws IOException {
		  Platform platform = PlatformFactory.createNewPlatformInstance(dts);
		  Database db = platform.readModelFromDatabase(dbName);
		  File ddl = new File("C:\\Clocial\\tmp\\"+dbName+"_ddl.xml");
		  FileOutputStream fos = new FileOutputStream(ddl, true);
		  new DatabaseIO().write(db, fos);
		  fos.flush();
		  fos.close();
	}

	@Ignore
	public void test02GenerateJdoSchema() throws JDOException, IOException {
		SchemaMetaData metadata = dts.getMetaData();
		JdoXmlMetadata xmlmeta = new JdoXmlMetadata(dts);
		File out = new File("C:\\Clocial\\tmp\\"+dbName+"_jdo.xml");
		FileOutputStream fos = new FileOutputStream(out,true);
		xmlmeta.writeMetadata(metadata, fos);
		fos.flush();
		fos.close();
	}

}
