package org.judal.benchmark.java.jdbc;

import static org.judal.storage.DataSource.DEFAULT_MAXPOOLSIZE;
import static org.judal.storage.DataSource.DEFAULT_POOLSIZE;
import static org.judal.storage.DataSource.DEFAULT_USE_DATABASE_METADATA;
import static org.judal.storage.DataSource.DRIVER;
import static org.judal.storage.DataSource.MAXPOOLSIZE;
import static org.judal.storage.DataSource.PASSWORD;
import static org.judal.storage.DataSource.POOLSIZE;
import static org.judal.storage.DataSource.SCHEMA;
import static org.judal.storage.DataSource.URI;
import static org.judal.storage.DataSource.USER;
import static org.judal.storage.DataSource.USE_DATABASE_METADATA;
import static org.judal.transaction.DataSourceTransactionManager.Transact;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.jdo.JDOException;

import org.judal.benchmark.java.MediumRecordData;
import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.JDBCRelationalDataSource;
import org.judal.jdbc.JDBCTableDataSource;
import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;
import org.judal.storage.relational.RelationalDataSource;

public class BenchmarkHelper {

	public static JDBCRelationalDataSource initialize() {
		Engine<JDBCRelationalDataSource> jdbc = new JDBCEngine();
		JDBCRelationalDataSource dts = jdbc.getDataSource(dataSourceProperties(), Transact);
		EngineFactory.DefaultThreadDataSource.set(dts);
		return dts;
	}

	public static void destroy() {
		RelationalDataSource dts = EngineFactory.getDefaultRelationalDataSource();
		dts.close();
		EngineFactory.DefaultThreadDataSource.set(null);
	}

	public static Map<String, String> dataSourceProperties() {
		
		// Properties for am HSQL DB in memory data source
		Map<String, String> properties = new HashMap<>();
		properties.put(DRIVER, "org.hsqldb.jdbc.JDBCDriver");
		properties.put(URI, "jdbc:hsqldb:mem:test");
		properties.put(USER, "sa");
		properties.put(PASSWORD, "");
		properties.put(POOLSIZE, DEFAULT_POOLSIZE);
		properties.put(MAXPOOLSIZE, DEFAULT_MAXPOOLSIZE);
		properties.put(SCHEMA, "PUBLIC");
		properties.put(USE_DATABASE_METADATA, DEFAULT_USE_DATABASE_METADATA);

		return properties;
	}
	
	public static void createSchemaObjects() throws JDOException, IOException, ClassNotFoundException, SQLException {
		StringBuilder ddl = new StringBuilder();
		ddl.append("CREATE TABLE " + MediumRecordData.TABLE_NAME + " (pk INTEGER NOT NULL");
		for (int i=0; i<=9; i++)
			ddl.append(", i"+i+" INTEGER NOT NULL");
		for (int d=0; d<=9; d++)
			ddl.append(", d"+d+" TIMESTAMP NOT NULL");
		for (int v=0; v<=49; v++)
			ddl.append(", v"+v+" VARCHAR(100) NOT NULL");
		ddl.append(", CONSTRAINT pk_" + MediumRecordData.TABLE_NAME + " PRIMARY KEY (pk))");
		
		JDBCTableDataSource dts = (JDBCTableDataSource) EngineFactory.getDefaultRelationalDataSource();
		dts.execute(ddl.toString());
		dts.restart();
	}

	public static void dropSchemaObjects() throws JDOException, IOException {
		JDBCTableDataSource dts = (JDBCTableDataSource) EngineFactory.getDefaultRelationalDataSource();
		dts.execute("DROP TABLE " + MediumRecordData.TABLE_NAME);
	}

	public static MediumRecordData[] generateMediumRecordData(final int howMany) {
		MediumRecordData[] data = new MediumRecordData[howMany];
		for (int s=0; s<howMany; s++) {
			data[s]= new MediumRecordData();
			data[s].pk = s + 1;
		}
		return data;
	}
	
}