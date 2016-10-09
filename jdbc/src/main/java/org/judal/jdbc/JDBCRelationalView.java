package org.judal.jdbc;

import java.lang.reflect.InvocationTargetException;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.metadata.ColumnDef;
import org.judal.storage.Param;
import org.judal.storage.Record;
import org.judal.storage.RecordSet;
import org.judal.storage.RelationalView;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.judal.storage.query.sql.SQLAndPredicate;
import org.judal.storage.query.sql.SQLQuery;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Connective;
import org.judal.storage.query.Operator;
import org.judal.storage.query.Predicate;

import static org.judal.storage.query.Operator.EQ;
import static org.judal.storage.query.Operator.GTE;
import static org.judal.storage.query.Operator.LTE;
import static org.judal.storage.query.Operator.BETWEEN;

import com.knowgate.debug.DebugFile;

public class JDBCRelationalView extends JDBCBucket implements RelationalView {

	private Class<? extends Record> recordClass;
	
	public JDBCRelationalView(JDBCTableDataSource dataSource, Record recordInstance) throws JDOException {
		super(dataSource, recordInstance.getTableName());
		recordClass = recordInstance.getClass();
	}

	public JDBCRelationalView(JDBCTableDataSource dataSource, SQLTableDef tableDef) throws JDOException {
		super(dataSource, tableDef);
	}

	@Override
	public ColumnDef[] columns() {
		return tableDef.getColumns();
	}

	@Override
	public int columnsCount() {
		return tableDef.getNumberOfColumns();
	}

	@Override
	public ColumnDef getColumnByName(String columnName) throws IllegalStateException {
		return tableDef.getColumnByName(columnName);
	}
	
	@Override
	public int getColumnIndex(String columnName) throws IllegalStateException {
		return tableDef.getColumnIndex(columnName);
	}

	@Override
	public Class<? extends Record> getResultClass() {
		return recordClass;
	}

	/**
	 * <p>Count number of rows having a given value for a column</p>
	 * @param indexColumnName String column name
	 * @param valueSearched Object Value at column
	 * @throws JDOException
	 */
	@Override
	public int count(String indexColumnName, Object valueSearched) throws JDOException {
		int rows = 0;
		ResultSet rset = null;
		PreparedStatement stmt = null;
		try {
			stmt = getConnection().prepareStatement("SELECT COUNT("+indexColumnName+") AS NUM_ROWS FROM "+name()+" WHERE "+indexColumnName+"=?");
			stmt.setObject(1, getTableDef().getColumnByName(indexColumnName).getType());
			rset = stmt.executeQuery();
			if (rset.next()) {
				rows = rset.getInt(1);
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
			if (first)
				first = false;
			else
				where.append(" AND ");
			where.append(key.getName()+"=?");
		}

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCTable.exists()");
			DebugFile.incIdent();
			DebugFile.writeln("Connection.prepareStatement(SELECT NULL AS void FROM "+name()+" WHERE "+where+")");
		}

		try {
			stmt = getConnection().prepareStatement("SELECT NULL AS void FROM "+name()+" WHERE "+where);
			int pos = 0;
			for (Param key : keys)
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
			DebugFile.writeln("End JDBCTable.exists() : "+String.valueOf(retval));
		}
		return retval;
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched) throws JDOException {
		return fetch(fetchGroup, indexColumnName, valueSearched, Integer.MAX_VALUE, 0);
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueFrom, Object valueTo) throws JDOException {
		return fetch(fetchGroup, indexColumnName, valueFrom, valueTo, Integer.MAX_VALUE, 0);
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched, int maxrows, int offset)
		throws JDOException {
		Predicate predicate = new SQLAndPredicate();
		try {
			predicate.add(indexColumnName, EQ, valueSearched);
		} catch (IllegalArgumentException | IllegalAccessException | UnsupportedOperationException | NoSuchMethodException | SecurityException | InstantiationException | InvocationTargetException xcpt) { 
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
		SQLQuery qry = newQuery();
		qry.setResult(fetchGroup.getMembers());
		qry.setFilter(predicate);
		qry.setRange(offset, offset+maxrows);
		return fetch(qry);		
	}
	
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
		SQLQuery qry = newQuery();
		qry.setResult(fetchGroup.getMembers());
		qry.setFilter(predicate);
		qry.setRange(offset, offset+maxrows);
		return fetch(qry);		
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(AbstractQuery qry)
		throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCIndexableView.fetch()");
			DebugFile.incIdent();
		}
		qry.setResultClass(recordClass);
		Object results = qry.execute();
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCIndexableView.fetch()");
		}
		return (RecordSet<R>) results;	
	} // fetch

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, int maxrows, int offset, Param... params)
			throws JDOException {
		SQLQuery qry = new SQLQuery(this);
		qry.setRange(offset, offset+maxrows);
		Predicate where = qry.newPredicate(Connective.AND);
		try {
			for (Param p : params)
				where.add(p.getName(), Operator.EQ, p.getValue());
		} catch (UnsupportedOperationException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException xcpt) {
			throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
		}
		return fetch(qry);
	}
	
	@Override
	public SQLQuery newQuery() throws JDOException {
		return new SQLQuery(this);
	}

}
