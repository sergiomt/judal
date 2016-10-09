package org.judal.jdbc;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 */

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import javax.jdo.metadata.ColumnMetadata;
import javax.jdo.metadata.PrimaryKeyMetadata;

import org.apache.poi.hssf.record.ArrayRecord;
import org.judal.jdbc.jdc.JDCConnection;
import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;
import org.judal.storage.Bucket;
import org.judal.storage.Param;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;

import org.judal.storage.Stored;
import org.judal.storage.AbstractRecord;

import com.knowgate.debug.DebugFile;

public class JDBCBucket implements Bucket {

	protected JDBCBucketDataSource dataSource;
	protected SQLTableDef tableDef;
	protected JDCConnection jdcConn;
	protected Class<Stored> candidateClass;
	protected Collection<JDBCIterator> iterators;

	public JDBCBucket(JDBCBucketDataSource dataSource, String bucketName) throws JDOException {
		this.dataSource = dataSource;
		tableDef = dataSource.getTableDef(bucketName);
		if (null==tableDef)
			throw new JDOException("Table "+bucketName+" not found");
		try {
			jdcConn = dataSource.getConnection(bucketName);
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
		iterators = null;
	}

	 public JDBCBucket(JDBCBucketDataSource dataSource, SQLTableDef tableDef) throws JDOException {
		this.dataSource = dataSource;
		this.tableDef = tableDef;
		try {
			jdcConn = dataSource.getConnection(tableDef.getName());
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
		candidateClass = null;
		iterators = null;
	}

	public JDCConnection getConnection() {
		return jdcConn;
	}

	public JDBCBucketDataSource getDataSource() {
		return dataSource;
	}
	
	public void setConnection(JDCConnection conn) {
		this.jdcConn = conn;
	}

	public SQLTableDef getTableDef() {
		return tableDef;
	}

	public void setTableDef(TableDef proxy) throws JDOException {
		try {
			tableDef = new SQLTableDef(RDBMS.valueOf(jdcConn.getDataBaseProduct()), proxy.getName(), proxy.getColumns());
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}
	
	@Override
	public String name() {
		return tableDef.getName();
	}

	public void commit() throws JDOException {
		try {
			getConnection().commit();
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}

	public void rollback() throws JDOException {
		try {
			getConnection().rollback();
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}
	
	@Override
	public void close() throws JDOException {
		try {
			jdcConn.close(tableDef.getName());
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}

	@Override
	public boolean load(Object key, Stored target) throws JDOException {
		boolean found = false;
		AbstractRecord mapRecord = (AbstractRecord) target;
		try {
			if (key.getClass().isArray())
				found = tableDef.loadRegister(jdcConn, (Object[]) key, mapRecord);
			else
				found = tableDef.loadRegister(jdcConn, new Object[] { key }, mapRecord);
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
		return found;
	}

	@Override
	public void store(Stored target) throws JDOException {
		boolean bHasLongVarBinaryData = false;
		AbstractRecord mapRecord = (AbstractRecord) target;
		
		if (null==target)
			throw new NullPointerException("JDBCBucket.store() Target instance may not be null");

		if (null==tableDef)
			throw new NullPointerException("JDBCBucket.store() Target TableDef may not be null");
		
		if (target instanceof AbstractRecord) {
			bHasLongVarBinaryData = ((AbstractRecord) target).hasLongData();
		}

		if (bHasLongVarBinaryData) {
			try {
				tableDef.storeRegisterLong(jdcConn, mapRecord, mapRecord.longDataLengths());
			} catch (IOException ioe) {
				throw new JDOException(ioe.getMessage(), ioe);
			} catch (SQLException sqle) {
				throw new JDOException(sqle.getMessage(), sqle);
			} finally {
				if (bHasLongVarBinaryData)
					mapRecord.clearLongData();
			}
		} else {
			try {
				tableDef.storeRegister(jdcConn, (AbstractRecord) target);
			} catch (SQLException sqle) {
				throw new JDOException(sqle.getMessage(), sqle);
			}
		}
	}

	/**
	 * <p>Check whether a register with a given primary key exists at the underlying table</p>
	 * @param key Object Primary Key Value
	 * @throws JDOException
	 */
	@Override
	public boolean exists(Object key) throws JDOException {
		boolean retval;
		ResultSet rset = null;
		PreparedStatement stmt = null;		
		PrimaryKeyMetadata pk = tableDef.getPrimaryKeyMetadata();

		if (pk.getNumberOfColumns()==0)
			throw new JDOException("Table "+name()+" has no primary key");
		else if (pk.getNumberOfColumns()>1)
			throw new JDOException("Table "+name()+" primary key is composed of more than 1 column");

		Object value = key instanceof Param ? ((Param) key).getValue() : key;
			
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDCConnection.exists("+value+")");
			DebugFile.incIdent();
			DebugFile.writeln("Connection.prepareStatement(SELECT NULL AS void FROM "+name()+" WHERE "+pk.getColumn()+"="+value+")");
		}

		int sqlType;
		if (value instanceof Boolean)
			sqlType = Types.BOOLEAN;
		else if (value instanceof Short)
			sqlType = Types.SMALLINT;
		else if (value instanceof Integer)
			sqlType = Types.INTEGER;
		else if (value instanceof Long)
			sqlType = Types.BIGINT;
		else if (value instanceof Float)
			sqlType = Types.FLOAT;
		else if (value instanceof Double)
			sqlType = Types.DOUBLE;
		else if (value instanceof BigDecimal)
			sqlType = Types.DECIMAL;
		else
			sqlType = Types.VARCHAR;

		try {
			stmt = jdcConn.prepareStatement("SELECT NULL AS void FROM "+name()+" WHERE "+pk.getColumn()+"=?");
			if (DebugFile.trace) DebugFile.writeln("PreparedStatement.setObject(1,"+value+","+ ColumnDef.typeName(sqlType) +")");
			stmt.setObject(1, value, sqlType);
			rset = stmt.executeQuery();
			retval = rset.next();
			rset.close();
			rset = null;
			stmt.close();
			stmt=null;
		} catch (SQLException sqle) {
			try { if (rset!=null) rset.close(); } catch (Exception ignore) { }
			try { if (stmt!=null) stmt.close(); } catch (Exception ignore) { }
			if (DebugFile.trace) DebugFile.decIdent();
			throw new JDOException(sqle.getMessage(), sqle);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDCConnection.exists() : "+String.valueOf(retval));
		}
		return retval;
	}

	@Override
	public void delete(Object key) throws JDOException {
		PrimaryKeyMetadata pk = tableDef.getPrimaryKeyMetadata();
		if (pk.getNumberOfColumns()==0)
			throw new JDOException("Cannot delete a single record because table "+name()+" has no primary key");
		else if (pk.getNumberOfColumns()!=1 && !key.getClass().isArray())
			throw new JDOException("Not enough values supplied for primary key");
		else if (pk.getNumberOfColumns()>1 && ((Object[]) key).length!=pk.getNumberOfColumns())
			throw new JDOException("Wrong number of values supplied for primary key");
		HashMap<String,Object> keymap = new HashMap<String,Object>(5);
		if (pk.getNumberOfColumns()==1) {
			keymap.put(pk.getColumn(), key);
		} else {
			Object[] keyvals = (Object[]) key;
			if (keyvals.length!=pk.getNumberOfColumns())
				throw new JDOException("Wrong number of columns expected "+String.valueOf(pk.getNumberOfColumns())+" but got"+String.valueOf(keyvals.length));
			int k = 0;
			for (ColumnMetadata colDef: pk.getColumns())			
				keymap.put(colDef.getName(), keyvals[k++]);
		}
		try {
			tableDef.deleteRegister(jdcConn, keymap);
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}

	@Override
	public void close(Iterator<Stored> iterator) {
		((JDBCIterator) iterator).close();
	}

	@Override
	public void closeAll() {
		if (null!=iterators)
			for (JDBCIterator iterator : iterators)
				iterator.close();
	}

	@Override
	public Class<Stored> getCandidateClass() {
		return candidateClass;
	}

	public void setCandidateClass(Class<Stored> candidateClass) {
		this.candidateClass = candidateClass;
	}
	
	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		return null;
	}

	@Override
	public boolean hasSubclasses() {
		return false;
	}

	@Override
	public Iterator<Stored> iterator() {
		JDBCIterator retval = null;
		if (null==iterators)
			iterators = new LinkedList<JDBCIterator>();
		PreparedStatement stmt = null;
		ResultSet rset = null;
		try {
			stmt = getConnection().prepareStatement("SELECT * FROM "+name(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			retval = new JDBCIterator(candidateClass!=null ? candidateClass : ArrayRecord.class, tableDef, stmt, rset);			
		} catch (SQLException | NoSuchMethodException | SecurityException xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
		return retval;
	}

	@Override
	public void setClass(Class<Stored> candidateClass) {
		this.candidateClass = candidateClass;		
	}

}
