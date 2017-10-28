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

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import javax.jdo.metadata.ColumnMetadata;
import javax.jdo.metadata.PrimaryKeyMetadata;

import org.judal.jdbc.jdc.JDCConnection;
import org.judal.jdbc.metadata.SQLHelper;
import org.judal.jdbc.metadata.SQLSelectableDef;
import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.storage.Param;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;

import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.Record;
import org.judal.storage.table.impl.AbstractRecord;

/**
 * <p>JDBC implementation of org.judal.storage.keyvalue.Bucket interface.</p>
 * Every JDBCBucket contains an org.judal.jdbc.jdc.JDCConnection from a pool.
 * The connection will be fetched from the pool when the JDBCBucket is constructed
 * and returned to the pool when the JDBCBucket is closed. Therefore JDBCBuckets
 * must be always closed in order to avoid potential connection leaks.
 * A relational table used as a bucket must have two columns "key" and "value".
 * The actual name of the columns at the RDBMS table may be specified when the
 * bucket is created using JDBCBucketDataSource.createBucket() or might have been created
 * by an external SQL-DDL script.
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JDBCBucket extends JDBCBase implements Bucket {

	protected JDBCBucketDataSource dataSource;
	protected SQLTableDef tableDef;
	protected String alias;
	protected Class<? extends Stored> candidateClass;
	protected Collection<JDBCIterator> iterators;

	/**
	 * <p>Constructor.</p>
	 * @param dataSource JDBCBucketDataSource
	 * @param bucketName String Table Name
	 * @throws JDOException if no table with given name can be found at dataSource
	 */
	public JDBCBucket(JDBCBucketDataSource dataSource, String bucketName) throws JDOException {
		super(dataSource, bucketName);
		this.dataSource = dataSource;
		tableDef = dataSource.getTableDef(bucketName);
		alias = null;
		if (null==tableDef)
			throw new JDOException("Table "+bucketName+" not found");
		iterators = null;
	}

	/**
	 * <p>Constructor.</p>
	 * @param dataSource JDBCBucketDataSource
	 * @param tableDef SQLTableDef
	 * @throws JDOException
	 */
	public JDBCBucket(JDBCBucketDataSource dataSource, SQLTableDef tableDef) throws JDOException {
		super(dataSource, tableDef.getName());
		this.dataSource = dataSource;
		this.tableDef = tableDef;
		candidateClass = null;
		iterators = null;
	}

	/**
	 * @return JDBCBucketDataSource
	 */
	public JDBCBucketDataSource getDataSource() {
		return dataSource;
	}
	
	/**
	 * <p>Set connection.</p>
	 * Use this method with care.
	 * Explicitly setting a new connection will not release the connection already in use by this instance.
	 * Therefore getConnection().close(getTableDef().getName()) must be called before explicitly setting a
	 * new connection in order to avoid a connection leak.
	 * @param conn JDCConnection
	 */
	public void setConnection(JDCConnection conn) {
		this.jdcConn = conn;
	}

	/**
	 * @return SQLSelectableDef
	 */
	public SQLSelectableDef getTableDef() {
		return tableDef;
	}

	/**
	 * <p>Set TableDef by cloning a given one.</p>
	 * @param proxy SQLSelectableDef
	 * @throws JDOException
	 */
	public void setTableDef(SQLSelectableDef proxy) throws JDOException {
		try {
			tableDef = new SQLTableDef(RDBMS.valueOf(jdcConn.getDataBaseProduct()), proxy.getName(), proxy.getColumns());
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}
	
	/**
	 * @return String
	 */
	public String getAlias() {
		return alias;
	}
	
	/**
	 * @param alias String
	 */
	public void setAlias(String alias) {
		this.alias = alias;
	}

	/**
	 * @return String Name
	 */
	@Override
	public String name() {
		if (null==getAlias())
			return tableDef.getName();
		else
			return tableDef.getName() + " " + getAlias();
	}

	/**
	 * <p>Commit transaction on this bucket connection.</p>
	 * @throws JDOException
	 */
	public void commit() throws JDOException {
		try {
			getConnection().commit();
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}

	/**
	 * <p>Rollback transaction on this bucket connection.</p>
	 * @throws JDOException
	 */
	public void rollback() throws JDOException {
		try {
			getConnection().rollback();
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}
	
	/**
	 * <p>Close all iterators and return the internal JDBC connection to the pool.</p>
	 * @throws JDOException
	 */
	@Override
	public void close() throws JDOException {
		closeAll();
		super.close();
	}

	/**
	 * <p>Load Stored instance from a table row.</p>
	 * If no row is found with the given primary key then target is not modified.
	 * @param key Object Primary key of row to be loaded.
	 * @param target Stored
	 * @return boolean <b>true</b> if a row with the given primary key was found
	 * @throws JDOException
	 */
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

	/**
	 * <p>Store into database.</p>
	 * Store will either update (if it already exists) or insert (if it does not exist) the value held by the Stored instance.
	 * @param target Stored
	 * @throws JDOException
	 */
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
	 * <p>Check whether a value with a given key exists at a table</p>
	 * @param key Object Primary Key Value
	 * @throws JDOException
	 */
	@Override
	public boolean exists(Object key) throws JDOException {
		PrimaryKeyMetadata pk = tableDef.getPrimaryKeyMetadata();

		if (pk.getNumberOfColumns()==0)
			throw new JDOException("Table "+name()+" has no primary key");
		else if (pk.getNumberOfColumns()>1)
			throw new JDOException("Table "+name()+" primary key is composed of more than 1 column");

		Object value = key instanceof Param ? ((Param) key).getValue() : key;
			
		try {
			return new SQLHelper(dataSource.getDatabaseProductId(), tableDef, null).existsRegister(jdcConn, " WHERE "+pk.getColumn()+"=?", new Object[] {value});
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
		
	}

	/**
	 * <p>Delete value with given key.</p>
	 * @param key Object
	 * @throws JDOException
	 */
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

	/**
	 * <p>Close a bucket iterator.</p>
	 * @param iterator JDBCIterator
	 * @throws ClassCastException if iterator is not instance of JDBCIterator 
	 */
	@Override
	public void close(Iterator<Stored> iterator) {
		((JDBCIterator) iterator).close();
	}

	/**
	 * <p>Close all iterators over this bucket.</p>
	 */
	@Override
	public void closeAll() {
		if (null!=iterators)
			for (JDBCIterator iterator : iterators)
				iterator.close();
	}

	/**
	 * @return Class&lt;Stored&gt;
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Class<Stored> getCandidateClass() {
		return (Class<Stored>) candidateClass;
	}

	/**
	 * @param candidateClass Class&lt;Stored&gt;
	 */
	public void setCandidateClass(Class<Stored> candidateClass) {
		this.candidateClass = candidateClass;
	}
	
	/**
	 * @return This function always returns <b>null</b>
	 */
	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}

	/**
	 * @return This function always returns <b>null</b>
	 */
	@Override
	public PersistenceManager getPersistenceManager() {
		return null;
	}

	/**
	 * @return This function always returns <b>false</b>
	 */
	@Override
	public boolean hasSubclasses() {
		return false;
	}

	/**
	 * <p>Open iterator.</p>
	 * Each iterator will open a new java.sql.ResultSet over the underlying table using this bucket connection.
	 * The ResultSet will remain open until the iterator is closed.
	 * @return JDBCIterator
	 */
	@Override
	@SuppressWarnings("unchecked")
	public JDBCIterator iterator() {
		JDBCIterator retval = null;
		if (null==iterators)
			iterators = new LinkedList<JDBCIterator>();
		PreparedStatement stmt = null;
		ResultSet rset = null;
		try {
			stmt = getConnection().prepareStatement("SELECT * FROM "+name(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rset = stmt.executeQuery();
			retval = new JDBCIterator((Class<? extends Record>) (candidateClass), tableDef, stmt, rset);
			iterators.add(retval);
		} catch (SQLException | NoSuchMethodException | SecurityException xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
		return retval;
	}

	/**
	 * @param candidateClass Class&lt;? extends Stored&gt;
	 */
	@Override
	public void setClass(Class<? extends Stored> candidateClass) {
		this.candidateClass = candidateClass;	
	}

}
