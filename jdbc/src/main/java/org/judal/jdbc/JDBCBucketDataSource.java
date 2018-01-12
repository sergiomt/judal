package org.judal.jdbc;

/**
 * © Copyright 2016 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

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

/**
 * <p>JDBC implementation of BucketDataSource</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JDBCBucketDataSource extends JDBCDataSource implements BucketDataSource {

	public JDBCBucketDataSource(Map<String, String> properties, TransactionManager transactManager)
		throws SQLException, NumberFormatException, ClassNotFoundException, NullPointerException, UnsatisfiedLinkError {
		super(properties, transactManager);
	}

	/**
	 * <p>Translate generic SQL into RDBMS specific SQL</p>
	 * Calls JDBCModelManager.translate to perform the translation.
	 * @param sql String
	 * @return String
	 * @throws JDOException
	 */
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
	
	
	/**
	 * <p>Execute SQL statement.</p>
	 * @param sql String
	 * @throws JDOException if this data source is in the middle of a transaction
	 */
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

	/**
	 * <p>Create a database table to act as a bucket.</p>
	 * The table will contain a VARCHAR(255) column as key and a LONGVARBINARY as value.
	 * @param bucketName String
	 * @param options Map&lt;String,Object&gt; Optional. If not <b>null</b> Then it may contain two entries named "key" and "value". The value of each of these entries will be the name given to the key and value columns respectively. 
	 * @throws JDOException
	 */
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
			keycolumn.setPrimaryKey(true);
			tblDef.addPrimaryKeyColumn("", keycolumn.getName(), Types.VARCHAR, 255);
			SQLColumn valcolumn = new SQLColumn(bucketName, valfield, (short) Types.LONGVARBINARY, "LONGVARBINARY", 2147483647, 0, DatabaseMetaData.columnNullable, 2);
			tblDef.addColumnMetadata(valcolumn);
			execute(tblDef.getSource());		
			cacheTableMetadata(tblDef);
			addColumnsToCache(tblDef, conn, conn.getMetaData());
			conn.close("JDBCBucketDataSource");
			conn = null;
		} catch (SQLException sqle) {
			try { if (conn!=null) conn.close("JDBCBucketDataSource"); } catch (Exception ignore) { }
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}

	/**
	 * <p>Open bucket.</p>
	 * When a Bucket is open it takes a connection from a pool and it the current thread has an active transaction then the connection is enlisted in the list of resources of the transaction
	 * @param bucketName String Table Name
	 * @throws JDOException
	 */
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

	/**
	 * <p>Drop table used as a bucket.</p>
	 * @param bucketName String Table Name
	 * @throws JDOException
	 */
	@Override
	public void dropBucket(String bucketName) throws JDOException {
		execute("DROP TABLE "+bucketName);
	}

	/**
	 * <p>Truncate table used as a bucket.</p>
	 * @param bucketName String Table Name
	 * @throws JDOException
	 */
	@Override
	public void truncateBucket(String bucketName) throws JDOException {
		execute("TRUNCATE TABLE "+bucketName);
	}

}
