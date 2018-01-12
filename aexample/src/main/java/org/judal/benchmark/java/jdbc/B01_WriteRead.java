package org.judal.benchmark.java.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

import javax.jdo.JDOException;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.openjdk.jmh.annotations.Benchmark;

import org.judal.benchmark.java.MediumRecordData;
import org.judal.benchmark.java.model.MediumRecordArray;
import org.judal.benchmark.java.model.MediumRecordMap;
import org.judal.benchmark.java.model.MediumRecordPojo;
import org.judal.jdbc.JDBCRelationalDataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.Param;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.Table;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.table.View;

import com.knowgate.debug.Chronometer;
import static com.knowgate.debug.DebugFile.trace;

public class B01_WriteRead {

	public static int howMany = 10000;

	boolean globalTrace;

	MediumRecordData[] data;
	
	SessionFactory sfact;
	
	@Before
	public void setUp() throws JDOException, ClassNotFoundException, IOException, SQLException {
		globalTrace = trace;
		trace = false;
		BenchmarkHelper.initialize();
		BenchmarkHelper.createSchemaObjects();
		sfact = HibernateHelper.getSessionFactory();
		data = BenchmarkHelper.generateMediumRecordData(howMany);
	}

	@After
	public void tearDown() {
		HibernateHelper.shutdown();
		BenchmarkHelper.destroy();
		trace = globalTrace;
	}

	@Test
	public void run() throws Exception {
		Chronometer ch = new Chronometer();
		JDBCRelationalDataSource dts = (JDBCRelationalDataSource) EngineFactory.getDefaultRelationalDataSource();

		insertJDBCMany(data);
		trunc(MediumRecordData.TABLE_NAME);

		ch.start();
		insertJDBCMany(data);
		ch.stop();
		System.out.println("Insert " + howMany + " rows using prepared JDBC took " + ch.elapsed() + " ms");

		ch.start();
		readJDBC();
		ch.stop();
		System.out.println("Read   " + howMany + " rows using JDBC took " + ch.elapsed() + " ms");
		
		trunc(MediumRecordData.TABLE_NAME);

		ch.start();
		insertJUDALMany(data);
		ch.stop();
		System.out.println("Insert " + howMany + " rows using direct JUDAL took " + ch.elapsed() + " ms");

		ch.start();
		readJUDALArray(dts);
		ch.stop();
		System.out.println("Read   " + howMany + " rows using JUDAL array took " + ch.elapsed() + " ms");
		
		ch.start();
		readJUDALMap(dts);
		ch.stop();
		System.out.println("Read   " + howMany + " rows using JUDAL map took " + ch.elapsed() + " ms");

		ch.start();
		readJUDALPojo(dts);
		ch.stop();
		System.out.println("Read   " + howMany + " rows using JUDAL pojo took " + ch.elapsed() + " ms");

		trunc(MediumRecordData.TABLE_NAME);

		ch.start();		
		for (int r = 0; r < data.length; r++) {
			insertJDBCOne(dts, data[r]);
		}
		ch.stop();
		System.out.println("Insert " + howMany + " rows using JDBC one by one took " + ch.elapsed() + " ms");
		
		trunc(MediumRecordData.TABLE_NAME);

		ch.start();		
		for (int r = 0; r < data.length; r++) {
			insertJUDALOneArray(dts, data[r]);
		}
		ch.stop();
		System.out.println("Insert " + howMany + " rows using JDBC one by one array took " + ch.elapsed() + " ms");

		trunc(MediumRecordData.TABLE_NAME);

		ch.start();		
		for (int r = 0; r < data.length; r++) {
			insertJUDALOneMap(dts, data[r]);
		}
		ch.stop();
		System.out.println("Insert " + howMany + " rows using JDBC one by one map took " + ch.elapsed() + " ms");

		trunc(MediumRecordData.TABLE_NAME);

		ch.start();		
		for (int r = 0; r < data.length; r++) {
			insertJUDALOneMap(dts, data[r]);
		}
		ch.stop();
		System.out.println("Insert " + howMany + " rows using JDBC one by one pojo took " + ch.elapsed() + " ms");

		trunc(MediumRecordData.TABLE_NAME);
		
		Session hses = sfact.openSession();
		ch.start();		
		for (int r = 0; r < data.length; r++) {
			insertHibernateOne(hses, data[r]);
		}
		hses.flush();
		hses.close();
		ch.stop();
		System.out.println("Insert " + howMany + " rows using Hibernate took " + ch.elapsed() + " ms");
	
		trunc(MediumRecordData.TABLE_NAME);

		ch.start();		
		for (int r = 0; r < data.length; r++) {
			hses = sfact.openSession();
			insertHibernateOne(hses, data[r]);
			hses.flush();
			hses.close();
		}
		ch.stop();
		System.out.println("Insert " + howMany + " rows using Hibernate one by one took " + ch.elapsed() + " ms");

		ch.start();
		hses = sfact.openSession();
		readHibernate(hses);
		hses.close();
		ch.stop();
		System.out.println("Read   " + howMany + " rows using Hibernate query took " + ch.elapsed() + " ms");
	}

	public void trunc(final String TableName) {
		EngineFactory.getDefaultRelationalDataSource().truncateTable(TableName, false);
	}

	@Benchmark
	public void insertJDBCOne(JDBCRelationalDataSource dts, MediumRecordData d) throws SQLException {

		final String sql = "INSERT INTO " + MediumRecordData.TABLE_NAME + " VALUES (?" + String.format("%0" + 70 + "d", 0).replace("0", ",?") + ")";
		Connection conn = dts.getConnection();
		PreparedStatement stmt = conn.prepareStatement(sql);

		int col = 0;
		stmt.setInt(++col, d.pk);
		for (int c = 0; c < d.ints.length; c++)
			stmt.setInt(++col, d.ints[c]);
		for (int c = 0; c < d.dates.length; c++)
			stmt.setTimestamp(++col, new Timestamp(d.dates[c].getTime()));
		for (int c = 0; c < d.varchars.length; c++)
			stmt.setString(++col, d.varchars[c]);
		stmt.executeUpdate();

		stmt.close();
		conn.close();
	}

	@Benchmark
	public void readJDBC() throws SQLException {
		JDBCRelationalDataSource dts = (JDBCRelationalDataSource) EngineFactory.getDefaultRelationalDataSource();
		MediumRecordData[] data = new MediumRecordData[howMany];
		Connection conn = dts.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rset = stmt.executeQuery("SELECT * FROM "+MediumRecordData.TABLE_NAME);
		int r = 0;
		while (rset.next()) {
			int n = 0;
			MediumRecordData d = data[r] = new MediumRecordData(false);
			d.pk = rset.getInt(++n);
			for (int c = 0; c < d.ints.length; c++)
				d.ints[c] = rset.getInt(++n);
			for (int c = 0; c < d.dates.length; c++)
				d.dates[c] = new Date(rset.getTimestamp(++n).getTime());
			for (int c = 0; c < d.varchars.length; c++)
				d.varchars[c] = rset.getString(++n);
			r++;
		}
		stmt.close();
		conn.close();
	}

	@Benchmark
	public void insertJDBCMany(MediumRecordData[] data) throws SQLException {
		JDBCRelationalDataSource dts = (JDBCRelationalDataSource) EngineFactory.getDefaultRelationalDataSource();

		final String sql = "INSERT INTO " + MediumRecordData.TABLE_NAME + " VALUES (?"
				+ String.format("%0" + 70 + "d", 0).replace("0", ",?") + ")";
		Connection conn = dts.getConnection();
		PreparedStatement stmt = conn.prepareStatement(sql);

		for (int r = 0; r < data.length; r++) {
			int col = 0;
			MediumRecordData d = data[r];
			stmt.setInt(++col, d.pk);
			for (int c = 0; c < d.ints.length; c++)
				stmt.setInt(++col, d.ints[c]);
			for (int c = 0; c < d.dates.length; c++)
				stmt.setTimestamp(++col, new Timestamp(d.dates[c].getTime()));
			for (int c = 0; c < d.varchars.length; c++)
				stmt.setString(++col, d.varchars[c]);
			stmt.executeUpdate();
		}

		stmt.close();
		conn.close();
	}

	@Benchmark
	public void insertJUDALMany(MediumRecordData[] data) throws SQLException {
		JDBCRelationalDataSource dts = (JDBCRelationalDataSource) EngineFactory.getDefaultRelationalDataSource();
		Table tbl = dts.openTable(new org.judal.storage.java.MapRecord(dts, MediumRecordData.TABLE_NAME));
		Param[] params = new Param[71];
		MediumRecordData d = data[0];
		int p = 0;
		params[p++] = new Param("pk", Types.INTEGER, 1, (Object) null);
		for (int c = 1; c <= d.ints.length; c++)
			params[p++] = new Param("i" + (c - 1), Types.INTEGER, c + 1, (Object) null);
		for (int c = 1; c <= d.dates.length; c++)
			params[p++] = new Param("d" + (c - 1), Types.TIMESTAMP, c + 1 + d.ints.length, (Object) null);
		for (int c = 1; c <= d.varchars.length; c++)
			params[p++] = new Param("v" + (c - 1), Types.TIMESTAMP, c + 1 + d.ints.length + d.dates.length,
					(Object) null);

		for (int r = 0; r < data.length; r++) {
			d = data[r];
			p = 0;
			params[p++].setValue(new Integer(d.pk));
			for (int c = 0; c < d.ints.length; c++)
				params[p++].setValue(new Integer(d.ints[c]));
			for (int c = 0; c < d.dates.length; c++)
				params[p++].setValue(new Timestamp(d.dates[c].getTime()));
			for (int c = 0; c < d.varchars.length; c++)
				params[p++].setValue(d.varchars[c]);
			tbl.insert(params);
		}
		tbl.close();
	}

	private void insertJUDALOne(TableDataSource dts, MediumRecordData d, Record r) {
		Table tbl = dts.openTable(r);
		Param[] params = new Param[71];
		int p = 0;
		params[p++] = new Param("pk", Types.INTEGER, 1, (Object) null);
		for (int c = 1; c <= d.ints.length; c++)
			params[p++] = new Param("i" + (c - 1), Types.INTEGER, c + 1, (Object) null);
		for (int c = 1; c <= d.dates.length; c++)
			params[p++] = new Param("d" + (c - 1), Types.TIMESTAMP, c + 1 + d.ints.length, (Object) null);
		for (int c = 1; c <= d.varchars.length; c++)
			params[p++] = new Param("v" + (c - 1), Types.TIMESTAMP, c + 1 + d.ints.length + d.dates.length,
					(Object) null);
		p = 0;
		params[p++].setValue(new Integer(d.pk));
		for (int c = 0; c < d.ints.length; c++)
			params[p++].setValue(new Integer(d.ints[c]));
		for (int c = 0; c < d.dates.length; c++)
			params[p++].setValue(new Timestamp(d.dates[c].getTime()));
		for (int c = 0; c < d.varchars.length; c++)
			params[p++].setValue(d.varchars[c]);
		tbl.insert(params);
		tbl.close();
	}

	@Benchmark
	public void insertJUDALOneArray(JDBCRelationalDataSource dts, MediumRecordData d) throws SQLException {
		insertJUDALOne(dts, d, new MediumRecordArray());
	}
	
	@Benchmark
	public void insertJUDALOneMap(JDBCRelationalDataSource dts, MediumRecordData d) throws SQLException {
		insertJUDALOne(dts, d, new MediumRecordMap());
	}

	@Benchmark
	public void insertJUDALOnePojo(JDBCRelationalDataSource dts, MediumRecordData d) throws SQLException {
		insertJUDALOne(dts, d, new MediumRecordPojo());
	}

	@SuppressWarnings("unused")
	@Benchmark
	public void readJUDALArray(TableDataSource dts) {
		View viw = dts.openView(new MediumRecordMap());
		RecordSet<MediumRecordArray> rset = viw.fetch(null, null, null);
		viw.close();
	}

	@SuppressWarnings("unused")
	@Benchmark
	public void readJUDALMap(TableDataSource dts) {
		View viw = dts.openView(new MediumRecordArray());
		RecordSet<MediumRecordArray> rset = viw.fetch(null, null, null);
		viw.close();
	}

	@SuppressWarnings("unused")
	@Benchmark
	public void readJUDALPojo(TableDataSource dts) {
		View viw = dts.openView(new MediumRecordPojo());
		RecordSet<MediumRecordPojo> rset = viw.fetch(null, null, null);
		viw.close();
	}

	@Benchmark
	public void insertHibernateOne(Session hses, MediumRecordData d) throws SQLException {
		MediumRecordPojo rec = new MediumRecordPojo(d);
		hses.save(rec);
	}

	@Benchmark
	public void readHibernate(Session hses) throws SQLException {
		Query qry = hses.createQuery("SELECT r FROM MediumRecordPojo AS r");
		qry.list();
	}
}