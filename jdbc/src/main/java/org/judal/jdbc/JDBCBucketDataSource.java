package org.judal.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.judal.jdbc.jdc.JDCConnection;
import org.judal.jdbc.metadata.SQLColumn;
import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.BucketDataSource;

import javax.jdo.JDOException;

import com.knowgate.debug.DebugFile;

public class JDBCBucketDataSource extends JDBCDataSource implements BucketDataSource {

	public JDBCBucketDataSource(Map<String, String> properties, TransactionManager transactManager)
		throws SQLException, NumberFormatException, ClassNotFoundException, NullPointerException, UnsatisfiedLinkError {
		super(properties, transactManager);
	}

	public String translate(String sql) throws JDOException {
		String translatedSql = sql;
		JDCConnection conn = null;
		try {
			conn = getConnection("JDBCBucketDataSource");
			translatedSql = JDBCModelManager.translate(sql, conn.getDataBaseProduct());
			conn.close("JDBCBucketDataSource");
			conn = null;
		} catch (SQLException sqle) {
			try { if (conn!=null) conn.close("JDBCBucketDataSource"); } catch (Exception ignore) { }
			throw new JDOException(sqle.getMessage(), sqle);
		}
		return translatedSql;
	}
	
	public void execute(String sql) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCBucketDataSource.execute("+sql+")");
			DebugFile.incIdent();
		}
		if (inTransaction())
			throw new JDOException("Cannot execute a direct SQL statement in the middle of a transaction");
		JDCConnection conn = null;
		Statement stmt = null;
		try {
			conn = getConnection("JDBCBucketDataSource");
			stmt = conn.createStatement();
			if (DebugFile.trace)
				DebugFile.writeln("Statement.execute("+sql+")");
			stmt.execute(sql);
			stmt.close();
			stmt = null;
			conn.close("JDBCBucketDataSource");
			conn = null;
		} catch (SQLException sqle) {
			if (DebugFile.trace) DebugFile.decIdent();
			try { if (stmt!=null) stmt.close(); } catch (Exception ignore) { }
			try { if (conn!=null) conn.close("JDBCBucketDataSource"); } catch (Exception ignore) { }
			throw new JDOException(sqle.getMessage()+"\n"+sql, sqle);
		}		
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCBucketDataSource.execute()");
		}
	}

	@Override
	public void createBucket(String bucketName, Map<String,Object> options) throws JDOException {
		String keyfield;
		String valfield;
		if (options==null) {
			keyfield = "key";
			valfield = "value";
		} else {
			keyfield = options.get("key")==null ? "key" : (String) options.get("key");
			valfield = options.get("value")==null ? "value" : (String) options.get("value");			
		}
		JDCConnection conn = null;
		String catalog = null;
		String schema = null;
		try {
			conn = getConnection("JDBCBucketDataSource");
			try {
				catalog = conn.getCatalog();
			} catch (SQLException notimplemented) { }
			try {
				schema = conn.getSchema();
			} catch (SQLException notimplemented) { }
			SQLTableDef tblDef = new SQLTableDef(getDatabaseProductId(), catalog, schema, bucketName);
			SQLColumn keycolumn = new SQLColumn(bucketName, keyfield, (short) Types.VARCHAR, "VARCHAR", 255, 0, DatabaseMetaData.columnNoNulls, 1);
			SQLColumn valcolumn = new SQLColumn(bucketName, valfield, (short) Types.LONGVARBINARY, "LONGVARBINARY", 2147483647, 0, DatabaseMetaData.columnNullable, 2);
			keycolumn.setPrimaryKey(true);
			tblDef.addColumnMetadata(keycolumn);
			tblDef.addColumnMetadata(valcolumn);
			execute(tblDef.getSource());		
			addTableToCache(tblDef);
			addColumnsToCache(tblDef, conn, conn.getMetaData());
			conn.close("JDBCBucketDataSource");
			conn = null;
		} catch (SQLException sqle) {
			try { if (conn!=null) conn.close("JDBCBucketDataSource"); } catch (Exception ignore) { }
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}

	@Override
	public Bucket openBucket(String bucketName) throws JDOException {
		JDBCBucket bckt = new JDBCBucket(this, bucketName);
		if (inTransaction()) {
			try {
				getTransactionManager().getTransaction().enlistResource(bckt.getConnection());
			} catch (IllegalStateException | RollbackException | SystemException e) {
				if (DebugFile.trace) DebugFile.writeln(e.getClass().getName()+" "+e.getMessage());
				bckt.close();
				throw new JDOException(e.getMessage(), e);
			}
		}
		return bckt;
	}

	@Override
	public void dropBucket(String bucketName) throws JDOException {
		execute("DROP TABLE "+bucketName);
	}

	@Override
	public void truncateBucket(String bucketName) throws JDOException {
		execute("TRUNCATE TABLE "+bucketName);
	}

}
