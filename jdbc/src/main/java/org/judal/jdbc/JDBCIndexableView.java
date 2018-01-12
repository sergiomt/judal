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

import java.lang.reflect.InvocationTargetException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import javax.jdo.FetchGroup;
import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;

import com.knowgate.debug.DebugFile;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.ViewDef;
import org.judal.storage.Param;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Connective;
import org.judal.storage.query.Expression;
import org.judal.storage.query.Operator;
import org.judal.storage.query.Predicate;
import org.judal.storage.query.sql.SQLAndPredicate;
import org.judal.storage.query.sql.SQLQuery;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.impl.AbstractRecord;
import org.judal.jdbc.jdc.JDCDAO;
import org.judal.jdbc.metadata.SQLBuilder;
import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.jdbc.metadata.SQLViewDef;

import static org.judal.storage.query.Operator.BETWEEN;
import static org.judal.storage.query.Operator.EQ;
import static org.judal.storage.query.Operator.GTE;
import static org.judal.storage.query.Operator.LTE;

/**
 * <p>Implementation of IndexableView.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JDBCIndexableView extends JDBCBase implements IndexableView {

	private SQLViewDef viewDef;
	protected JDBCTableDataSource dataSource;
	protected JDCDAO dao;
	protected Class<? extends Stored> candidateClass;
	protected Collection<JDBCIterator> iterators;
	private Class<? extends Record> recordClass;
	
	/**
	 * <p>Constructor.</p>
	 * @param dataSource JDBCTableDataSource
	 * @param recordInstance Record Instance of the Record implementation that will be used to fetch, load, store and delete records from the RDBMS view.
	 * @throws JDOException
	 */
	public JDBCIndexableView(JDBCTableDataSource dataSource, Record recordInstance) throws JDOException {
		super(dataSource, recordInstance.getTableName());
		this.dataSource = dataSource;
		candidateClass = recordClass = recordInstance.getClass();
		ViewDef tov = dataSource.getTableOrViewDef(recordInstance.getTableName());
		if (tov instanceof SQLViewDef)
			viewDef = (SQLViewDef) tov;
		else if (tov instanceof SQLTableDef)
			viewDef = ((SQLTableDef) tov).asView();
		if (null==viewDef)
			throw new JDOException("JDBCIndexableView Table or View not found "+recordInstance.getTableName());
		dao = null;
	}

	/**
	 * <p>Constructor.</p>
	 * @param dataSource JDBCTableDataSource
	 * @param viewDef SQLViewDef
	 * @param recClass Class&lt;? extends Record&gt; Class of the Record implementation that will be used to fetch, load, store and delete records from the RDBMS table.
	 * @throws JDOException
	 * @throws NullPointerException
	 */
	public JDBCIndexableView(JDBCTableDataSource dataSource, SQLViewDef viewDef, Class<? extends Record> recClass) throws JDOException, NullPointerException {
		super(dataSource, viewDef.getName());
		this.dataSource = dataSource;
		this.viewDef = viewDef;
		iterators = null;		
		candidateClass = recordClass = recClass;
		dao = null;
	}	

	/**
	 * <p>Get data access object for this view.</p>
	 * The DAO may be provided by the DataSource or created once on the fly for each view.
	 * @return JDCDAO
	 * @throws SQLException
	 */
	public JDCDAO getDao() throws SQLException {
		if (null==dao) {
			if (dataSource.daos.containsKey(viewDef.getName())) {
				dao = dataSource.daos.get(viewDef.getName());
			} else {
				dao = new JDCDAO(viewDef, new SQLBuilder(dataSource.getDatabaseProductId(), viewDef, null).getSqlStatements());
			}			
		}
		return dao;
	}

	/**
	 * <p>Get view alias.</p>
	 * @return String
	 */
	public String getAlias() {
		return getViewDef().getAlias();
	}
	
	/**
	 * <p>Set view alias.</p>
	 * @param alias String
	 */
	public void setAlias(String alias) {
		getViewDef().setAlias(alias);
	}
	
	/**
	 * @return JDBCTableDataSource
	 */
	public JDBCTableDataSource getDataSource() {
		return dataSource;
	}
	
	/**
	 * <p>Get columns of the ViewDef used to describe this IndexableView.</p>
	 * @return ColumnDef[]
	 */
	@Override
	public ColumnDef[] columns() {
		return viewDef.getColumns();
	}

	/**
	 * <p>Get count of columns of the ViewDef used to describe this IndexableView.</p>
	 * @return int
	 */
	@Override
	public int columnsCount() {
		return viewDef.getNumberOfColumns();
	}

	/**
	 * @return ColumnDef
	 */
	@Override
	public ColumnDef getColumnByName(String columnName) throws IllegalStateException {
		return viewDef.getColumnByName(columnName);
	}
	
	/**
	 * @return int
	 */
	@Override
	public int getColumnIndex(String columnName) throws IllegalStateException {
		return viewDef.getColumnIndex(columnName);
	}

	/**
	 * @return Class&lt;? extends Record&gt;
	 */
	@Override
	public Class<? extends Record> getResultClass() {
		return recordClass;
	}

	/**
	 * @param recordClass Class&lt;? extends Record&gt;
	 */
	public void setResultClass(Class<? extends Record> recordClass) {
		this.recordClass = recordClass;
	}

	/**
	 * @return SQLViewDef
	 */
	public SQLViewDef getViewDef() {
		return viewDef;
	}

	/**
	 * <p>Count number of rows having a given value for a column</p>
	 * @param indexColumnName String column name
	 * @param valueSearched Object Value at column
	 * @throws JDOException
	 */
	@Override
	public long count(String indexColumnName, Object valueSearched) throws JDOException {
		long rows = 0;
		ResultSet rset = null;
		PreparedStatement stmt = null;
		try {
			if (null==indexColumnName || indexColumnName.length()==0) {
				stmt = getConnection().prepareStatement("SELECT COUNT(*) AS NUM_ROWS FROM "+name());
			} else {
				stmt = getConnection().prepareStatement("SELECT COUNT("+indexColumnName+") AS NUM_ROWS FROM "+name()+" WHERE "+indexColumnName+"=?");
				ColumnDef cdef = getViewDef().getColumnByName(indexColumnName);
				if (null==valueSearched) {
					if (null==cdef)
						throw new JDOException("Type could not be infered for null value");
					else
						stmt = getConnection().prepareStatement("SELECT COUNT("+indexColumnName+") AS NUM_ROWS FROM "+name()+" WHERE "+indexColumnName+" IS NULL");
				} else {
					if (valueSearched instanceof Expression) {
						stmt = getConnection().prepareStatement("SELECT COUNT("+indexColumnName+") AS NUM_ROWS FROM "+name()+" WHERE "+indexColumnName+"="+valueSearched.toString());
					} else {
						stmt = getConnection().prepareStatement("SELECT COUNT("+indexColumnName+") AS NUM_ROWS FROM "+name()+" WHERE "+indexColumnName+"=?");
						if (null==cdef)
							stmt.setObject(1, valueSearched);
						else
							stmt.setObject(1, valueSearched, cdef.getType());
					}
				}
			}
			rset = stmt.executeQuery();
			if (rset.next()) {
				rows = rset.getLong(1);
				if (rset.wasNull())
					rows = 0;
			}
			rset.close();
			rset = null;
			stmt.close();
			stmt = null;
		} catch (SQLException sqle) {
			try { if (rset!=null) rset.close(); } catch (Exception ignore) { }
			try { if (stmt!=null) stmt.close(); } catch (Exception ignore) { }
			if (DebugFile.trace) DebugFile.decIdent();
			throw new JDOException(sqle.getMessage(), sqle);
		}
		return rows;
	}

	/**
	 * <p>Check whether a register matching the given search criteria exists at the underlying table</p>
	 * @param sKey Primary Key Value
	 * @throws JDOException
	 */
	@Override
	public boolean exists(Param... keys) throws JDOException {
		boolean retval;
		ResultSet rset = null;
		PreparedStatement stmt = null;

		StringBuffer where = new StringBuffer(100);
		boolean first = true;
		for (Param key : keys) {
			if (key!=null) {
				if (first)
					first = false;
				else
					where.append(" AND ");
				where.append(key.getName()+"=?");
			}
		}

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCRelationalView.exists()");
			DebugFile.incIdent();
			DebugFile.writeln("Connection.prepareStatement(SELECT NULL AS void FROM "+name()+" WHERE "+where+")");
		}

		try {
			stmt = getConnection().prepareStatement("SELECT NULL AS void FROM "+name()+" WHERE "+where);
			int pos = 0;
			for (Param key : keys)
				if (key!=null)
					stmt.setObject(++pos, key.getValue(), key.getType());
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
			DebugFile.writeln("End JDBCRelationalView.exists() : "+String.valueOf(retval));
		}
		return retval;
	}

	/**
	 * <p>Fetch RecordSet filtering by a column value.</p>
	 * @param fetchGroup FetchGroup Columns to fetch
	 * @param indexColumnName String Index column Name
	 * @param valueSearched Object value that the the fetched records must have
	 * @return RecordSet&lt;? extends Record&gt;
	 * @throws JDOException
	 */
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched) throws JDOException {
		return fetch(fetchGroup, indexColumnName, valueSearched, Integer.MAX_VALUE, 0);
	}

	/**
	 * <p>Fetch RecordSet filtering by a column value range.</p>
	 * @param fetchGroup FetchGroup Columns to fetch
	 * @param indexColumnName String Index column Name
	 * @param valueFrom Object value from (inclusive)
	 * @param valueTo Object value to (inclusive)
	 * @return RecordSet&lt;? extends Record&gt;
	 * @throws JDOException
	 */
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueFrom, Object valueTo) throws JDOException {
		return fetch(fetchGroup, indexColumnName, valueFrom, valueTo, Integer.MAX_VALUE, 0);
	}

	/**
	 * <p>Fetch RecordSet filtering by a column value.</p>
	 * @param fetchGroup FetchGroup Columns to fetch
	 * @param indexColumnName String Index column Name
	 * @param valueSearched Object value that the the fetched records must have
	 * @param maxrows int Maximum numbers of records to return
	 * @param offset int First record to read from [0..n]
	 * @return RecordSet&lt;? extends Record&gt;
	 * @throws JDOException
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched, int maxrows, int offset)
		throws JDOException {
		SQLQuery qry = new SQLQuery(this);
		if (null==fetchGroup)
			qry.setResult("*");
		else
			qry.setResult(fetchGroup.getMembers());
		qry.setRange(offset, offset+maxrows);			
		if (null==indexColumnName) {
			qry.setFilter((String) null);
		} else {
			Predicate predicate = new SQLAndPredicate();
			try {
				predicate.add(indexColumnName, EQ, valueSearched);
			} catch (IllegalArgumentException | IllegalAccessException | UnsupportedOperationException | NoSuchMethodException | SecurityException | InstantiationException | InvocationTargetException xcpt) { 
				throw new JDOException(xcpt.getMessage(), xcpt);
			}
			qry.setFilter(predicate);
		}
		return fetchQuery(qry);		
	}
	
	/**
	 * <p>Fetch RecordSet filtering by a column value range.</p>
	 * @param fetchGroup FetchGroup Columns to fetch
	 * @param indexColumnName String Index column Name
	 * @param valueFrom Object value from (inclusive)
	 * @param valueTo Object value to (inclusive)
	 * @param maxrows int Maximum numbers of records to return
	 * @param offset int First record to read from [0..n]
	 * @return RecordSet&lt;? extends Record&gt;
	 * @throws JDOException
	 */
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueFrom, Object valueTo, int maxrows, int offset)
		throws JDOException {
		Predicate predicate = new SQLAndPredicate();
		try {
			if (valueFrom==null && valueTo==null) {
				throw new JDOException("Range fetch needs at least value from or value to");
			} else if (valueFrom==null) {
				predicate.add(indexColumnName, LTE, valueTo);
			} else if (valueTo==null) {
				predicate.add(indexColumnName, GTE, valueFrom);
			} else if (!valueFrom.getClass().equals(valueTo.getClass())) {
				throw new JDOException("Range fetch needs value from and value to of the same type");
			} else {
				predicate.add(indexColumnName, BETWEEN, new Object[]{valueFrom, valueTo});
			}
		} catch (IllegalArgumentException | IllegalAccessException | UnsupportedOperationException | NoSuchMethodException | SecurityException | InstantiationException | InvocationTargetException xcpt) { 
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
		SQLQuery qry = new SQLQuery(this);
		if (null==fetchGroup)
			qry.setResult("*");
		else
			qry.setResult(fetchGroup.getMembers());
		qry.setFilter(predicate);
		qry.setRange(offset, offset+maxrows);
		return fetchQuery(qry);		
	}

	/**
	 * <p>Fetch RecordSet filtering by several column values.</p>
	 * @param fetchGroup FetchGroup Columns to fetch
	 * @param maxrows int Maximum numbers of records to return
	 * @param offset int First record to read from [0..n]
	 * @param params Param&hellip; Each Param name must match a column name in the table
	 * @throws JDOException
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, int maxrows, int offset, Param... params)
			throws JDOException {
		SQLQuery qry = new SQLQuery(this);
		qry.setRange(offset, offset+maxrows);
		if (null==fetchGroup)
			qry.setResult("*");
		else
			qry.setResult(fetchGroup.getMembers());
		if (params==null || params.length==0) {
			qry.setFilter((String) null);
		} else {
			Predicate where = qry.newPredicate(Connective.AND);
			try {
				for (Param p : params)
					where.add(p.getName(), Operator.EQ, p.getValue());
			} catch (UnsupportedOperationException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException xcpt) {
				throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
			}			
		}
		return fetchQuery(qry);
	}
	
	/**
	 * <p>Fetch RecordSet filtering records with a query.</p>
	 * @param qry AbstractQuery
	 * @throws JDOException
	 */
	@SuppressWarnings("unchecked")
	protected <R extends Record> RecordSet<R> fetchQuery(AbstractQuery qry)
		throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCIndexableView.fetchQuery(AbstractQuery)");
			DebugFile.incIdent();
		}
		qry.setResultClass(recordClass, getDataSource().getClass());
		Object results = qry.execute();
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCIndexableView.fetchQuery()");
		}
		return (RecordSet<R>) results;	
	} // fetch

	/**
	 * @return String Name
	 */
	@Override
	public String name() {
		if (null==getAlias())
			return viewDef.getName();
		else
			return viewDef.getName() + " " + getAlias();
	}

	/**
	 * <p>Check whether a record with given key exists at database table or view.</p>
	 * @param key Object May be Param[] or Object[] if the key has several columns
	 * @throws JDOException
	 */
	@Override
	public boolean exists(Object key) throws JDOException {
		boolean bExists = false;
		try {
			if (key instanceof Param[]) {
				Param[] peys = (Param[]) key;
				if (peys.length<1)
					throw new JDOException("Key must have at least one value");
				StringBuilder queryString = new StringBuilder();
				queryString.append(peys[0].getName()).append("=?");
				for (int p=1; p<peys.length; p++)
					queryString.append(" AND ").append(peys[0].getName()).append("=?");			
				try (PreparedStatement stmt = jdcConn.prepareStatement("SELECT NULL FROM " + name() + " WHERE " + queryString.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
					for (int p=0; p<peys.length; p++)
						stmt.setObject(p+1, peys[p].getValue(), peys[p].getType());
					ResultSet rset = stmt.executeQuery();
					bExists = rset.next();
					rset.close();				
				}
			} else if (key.getClass().isArray()) {
				Object[] keys = (Object[]) key;
				if (keys.length<1)
					throw new JDOException("Key must have at least one value");				
				if (viewDef.getPrimaryKeyMetadata()==null)
					throw new JDOException("View "+name()+" has no primary key");
				if (viewDef.getPrimaryKeyMetadata().getNumberOfColumns()!=keys.length)
					throw new JDOException("Expected "+String.valueOf(viewDef.getPrimaryKeyMetadata().getNumberOfColumns())+" key column but got "+String.valueOf(keys.length));
				StringBuilder queryString = new StringBuilder();
				bExists = getDao().existsRegister(jdcConn, queryString.toString(), keys);
			} else {
				bExists = getDao().existsRegister(jdcConn, viewDef.getPrimaryKeyMetadata().getColumn() + " =?", new Object[]{key});
			}
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
		return bExists;
	}

	/**
	 * <p>Load Record instance from a table row.</p>
	 * If no row is found with the given primary key then target is not modified.
	 * @param key Object Primary key of row to be loaded.
	 * @param target Record
	 * @return boolean <b>true</b> if a row with the given primary key was found
	 * @throws JDOException
	 */
	@Override
	public boolean load(Object key, Stored target) throws JDOException {
		boolean found = false;
		AbstractRecord mapRecord = (AbstractRecord) target;
		try {
			if (key.getClass().isArray())
				found = getDao().loadRegister(jdcConn, (Object[]) key, mapRecord);
			else
				found = getDao().loadRegister(jdcConn, new Object[] { key }, mapRecord);
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
		return found;
	}

	/**
	 * @param candidateClass Class&lt;? extends Record&gt;
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void setClass(Class<? extends Stored> candidateClass) {
		this.candidateClass = (Class<? extends Record>) candidateClass;
	}

	/**
	 * <p>Open iterator.</p>
	 * Each iterator will open a new java.sql.ResultSet over the underlying table using this view connection.
	 * The ResultSet will remain open until the iterator is closed.
	 * @return JDBCIterator
	 */
	@Override
	public JDBCIterator iterator() {
		JDBCIterator retval = null;
		if (null==iterators)
			iterators = new LinkedList<JDBCIterator>();
		PreparedStatement stmt = null;
		ResultSet rset = null;
		try {
			stmt = getConnection().prepareStatement("SELECT * FROM "+name(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rset = stmt.executeQuery();
			retval = new JDBCIterator(candidateClass, viewDef, stmt, rset);
			iterators.add(retval);
		} catch (SQLException | NoSuchMethodException | SecurityException xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
		return retval;
	}

	/**
	 * @return This function always returns <b>false</b>
	 */
	public boolean hasSubclasses() {
		return false;
	}

	/**
	 * @return Class&lt;Record&gt;
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Class<Stored> getCandidateClass() {
		return (Class<Stored>) candidateClass;
	}

	/**
	 * @return This function always returns <b>null</b>
	 */
	@Override
	public PersistenceManager getPersistenceManager() {
		return null;
	}

	/**
	 * <p>Close all iterators over this view.</p>
	 */
	@Override
	public void closeAll() {
		if (null!=iterators)
			for (JDBCIterator iterator : iterators)
				iterator.close();		
	}

	/**
	 * <p>Close a view iterator.</p>
	 * @param iterator JDBCIterator
	 * @throws ClassCastException if iterator is not instance of JDBCIterator 
	 */
	@Override
	public void close(Iterator<Stored> iterator) {
		((JDBCIterator) iterator).close();
	}

	/**
	 * @return This function always returns <b>null</b>
	 */
	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}
	
}
