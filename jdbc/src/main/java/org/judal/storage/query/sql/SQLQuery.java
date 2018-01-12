package org.judal.storage.query.sql;

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

import java.lang.reflect.Constructor;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

import java.util.Arrays;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;

import org.judal.storage.StorageObjectFactory;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Connective;
import org.judal.storage.query.Expression;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.SingleColumnRecord;
import org.judal.jdbc.RDBMS;
import org.judal.jdbc.JDBCIndexableView;

import com.knowgate.debug.DebugFile;
import com.knowgate.debug.StackTraceUtil;

public class SQLQuery extends AbstractQuery {

	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("rawtypes")
	private static Class ArrayRecordJava;
	@SuppressWarnings("rawtypes")
	private static Class ArrayRecordScala;

	static {
		try {
			ArrayRecordJava = Class.forName("org.judal.storage.java.ArrayRecord");
		} catch (ClassNotFoundException ignore) { }
		try {
			ArrayRecordScala = Class.forName("org.judal.storage.scala.ArrayRecord");
		} catch (ClassNotFoundException ignore) { }
	}

	private Constructor<? extends Record> recordConstructor;
	private Object[] constructorParameters;
	
	public SQLQuery(JDBCIndexableView view) throws JDOException {
		if (view.getResultClass()==null)
			throw new NullPointerException("SQLQuery() IndexableView.getResultClass() cannot be null");
		setCandidates(view);
		setRange(0, Integer.MAX_VALUE);
		recordConstructor = null;
		setResultClass(view.getResultClass(), view.getDataSource().getClass(), view.getClass());
	}

	@Override
	public SQLQuery clone() {
		SQLQuery theClone = new SQLQuery(getView());
		super.clone(this);
		theClone.recordConstructor = this.recordConstructor;
		if (constructorParameters==null)
			theClone.constructorParameters = null;
		else
			theClone.constructorParameters = Arrays.copyOf(constructorParameters, constructorParameters.length);
		return theClone;
	}
	
	private JDBCIndexableView getView() {
		return (JDBCIndexableView) getCandidates();
	}
	
	/**
	 * <p>Change transaction isolation level.</p>
	 * If <b>true</b> then the transaction isolation level will be set to
	 * TRANSACTION_SERIALIZABLE before fetching results, otherwise the
	 * transaction isolation level will be the default of the database
	 * in use which is usually TRANSACTION_READ_COMMITTED
	 * @param Boolean serialize
	 */
	@Override
	public void setSerializeRead(Boolean serialize) {
		super.setSerializeRead(serialize);
	}
	
	@Override
	public String getResult() {
		final String retval = super.getResult();
		return retval==null ? "*" : retval;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Record newRecord() {
		if (null==recordConstructor) {
			JDBCIndexableView v = getView();
			recordConstructor = (Constructor<? extends Record>) StorageObjectFactory.getConstructor(getResultClass(), new Class<?>[]{v.getDataSource().getClass(), v.getViewDef().getClass()});
			try {
				constructorParameters = StorageObjectFactory.filterParameters(recordConstructor.getParameters(), new Object[]{getView().getDataSource(), getView().getViewDef()});
			} catch (InstantiationException e) {
				StringBuilder constructorSignature = new StringBuilder();
				constructorSignature.append(getResultClass()==null ? "null" : getResultClass().getName());
				constructorSignature.append("(");
				constructorSignature.append(getView().getDataSource().getClass().getName());
				constructorSignature.append(",");
				constructorSignature.append(getView().getViewDef().getClass().getName());
				constructorSignature.append(")");
				if (DebugFile.trace)
					DebugFile.writeln(e.getMessage() + " getting constructor " + constructorSignature.toString());
				throw new JDOException(e.getMessage() + " getting constructor " + constructorSignature.toString(), e);
			}
		}
		return StorageObjectFactory.newRecord(recordConstructor, constructorParameters);
	}

	@Override
	public SQLPredicate newPredicate(Connective logicalConnective) {
		switch(logicalConnective) {
		case AND:
			return new SQLAndPredicate();
		case OR:
			return new SQLOrPredicate();
		default:
			return new SQLPredicate();
		}
	}

	private boolean supportLimitOffset() throws SQLException {
		final int dbms = getView().getConnection().getDataBaseProduct();
		return dbms==RDBMS.POSTGRESQL.intValue() || dbms==RDBMS.MYSQL.intValue() || dbms==RDBMS.HSQLDB.intValue();
	}
	
	public void setParameters(PreparedStatement stmt, int offset) throws SQLException {		
		int p = offset;
		for (Object oParam : getParameters()) {
			if (null==oParam) {
				ParameterMetaData oParMDat = stmt.getParameterMetaData();
				int pType;
				try {
					pType = oParMDat.getParameterType(p);
				} catch (SQLException parameternotavailable) {
					if (DebugFile.trace) DebugFile.writeln("JDBCQuery.execute() : SQLException "+parameternotavailable.getMessage()+" at ParameterMetaData.getParameterType("+String.valueOf(p+1)+")");
					pType = Types.NULL;
				}
				if (DebugFile.trace) DebugFile.writeln("PreparedStatement.setNull("+String.valueOf(p)+","+String.valueOf(pType)+")");
				stmt.setNull(p++, pType);
			} else if (oParam instanceof Expression) {
				if (DebugFile.trace) DebugFile.writeln("acknowledge expression "+oParam.toString());
				p++;
			} else {
				if (DebugFile.trace) DebugFile.writeln("PreparedStatement.setObject("+String.valueOf(p)+","+oParam.toString()+")");
				stmt.setObject(p++, oParam);
			} // fi
		} // next
	}

	public void setParameters(PreparedStatement stmt) throws SQLException {		
		setParameters(stmt,1);
	}

	@Override
	public RecordSet<? extends Record> execute() {
		PreparedStatement stmt = null;
		ResultSet rset = null;
		RecordSet<? extends Record> retval;
		int formerIsolationLevel = java.sql.Connection.TRANSACTION_READ_COMMITTED;

		long qtime = 0;

		if (DebugFile.trace)
			DebugFile.writeln("Begin SQLQuery.execute(" + getRangeFromIncl() + "," + getRangeToExcl() + ")");

		if (getRangeFromIncl()<0l)
			throw new IllegalArgumentException("row offset must be equal to or greater than zero");

		if (DebugFile.trace)
			DebugFile.incIdent();

		try {
			if (getSerializeRead()) {
				formerIsolationLevel = getView().getConnection().getTransactionIsolation();
				getView().getConnection().setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
			}

			stmt = prepareSelect();

			setParameters(stmt);

			if (getDatastoreReadTimeoutMillis()!=null && getDatastoreReadTimeoutMillis()>0) {
				if (DebugFile.trace) DebugFile.writeln("PreparedStatement.setQueryTimeout(" + String.valueOf(getDatastoreReadTimeoutMillis()*1000) + ")");
				stmt.setQueryTimeout(getDatastoreReadTimeoutMillis()*1000);
			}

			if (DebugFile.trace) {
				DebugFile.writeln("PreparedStatement.executeQuery()");
				qtime = System.currentTimeMillis();
			}

			rset = stmt.executeQuery();

			if (DebugFile.trace) {
				DebugFile.writeln("query executed in " + String.valueOf(System.currentTimeMillis()-qtime) + " ms");
			}

			setFetchSize(rset);

			try {
				retval = fetchResultSet(rset);
			} catch (NoSuchMethodException e) {
				throw new JDOException(e.getMessage(), e);
			}

			if (DebugFile.trace) DebugFile.writeln("ResultSet.close()");

			rset.close();
			rset = null;

			if (DebugFile.trace) DebugFile.writeln("PreparedStatement.close()");

			stmt.close();
			stmt = null;

			if (getSerializeRead())
				getView().getConnection().setTransactionIsolation(formerIsolationLevel);

		}
		catch (SQLException sqle) {
			try { 
				if (DebugFile.trace) DebugFile.writeln("SQLException "+sqle.getMessage()+"\n"+StackTraceUtil.getStackTrace(sqle));
			} catch (java.io.IOException ignore) {}
			try { if (null!=rset) rset.close();
			} catch (Exception logit) { if (DebugFile.trace) DebugFile.writeln(logit.getClass().getName()+" "+logit.getMessage()); }
			try { if (null!=stmt) stmt.close();
			} catch (Exception logit) { if (DebugFile.trace) DebugFile.writeln(logit.getClass().getName()+" "+logit.getMessage()); }
			try {
				if (getSerializeRead())
					getView().getConnection().setTransactionIsolation(formerIsolationLevel);
			} catch (Exception logit) { if (DebugFile.trace) DebugFile.writeln(logit.getClass().getName()+" "+logit.getMessage()); }
			if (getParameters().length==0)
				throw new JDOException(sqle.getSQLState()+" "+sqle.getMessage()+" zero parameters set", sqle);
			else {
				throw new JDOException(sqle.getSQLState()+" "+sqle.getMessage(), sqle);
			}
		}
		catch (IllegalAccessException | InstantiationException | ArrayIndexOutOfBoundsException xcpt) {
			try { 
				if (DebugFile.trace) DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage()+"\n"+StackTraceUtil.getStackTrace(xcpt));
			} catch (java.io.IOException ignore) {}
			try { if (null!=rset) rset.close();
			} catch (Exception logit) { if (DebugFile.trace) DebugFile.writeln(logit.getClass().getName()+" "+logit.getMessage()); }
			try { if (null!=stmt) stmt.close();
			} catch (Exception logit) { if (DebugFile.trace) DebugFile.writeln(logit.getClass().getName()+" "+logit.getMessage()); }
			try {
				if (getSerializeRead())
					getView().getConnection().setTransactionIsolation(formerIsolationLevel);
			} catch (Exception logit) { if (DebugFile.trace) DebugFile.writeln(logit.getClass().getName()+" "+logit.getMessage()); }
			if (getParameters().length==0)
				throw new JDOException(xcpt.getMessage()+" zero parameters set", xcpt);
			else {
				throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
			}
		}

		if (DebugFile.trace)
		{
			DebugFile.decIdent();
			DebugFile.writeln("End SQLQuery.execute() : "+String.valueOf(retval.size()));
		}

		return retval;
	} // execute
	
	@Override
	public String source() throws JDOException {
		int dbms;
		if (getView()==null)
			throw new JDOException("SQLQuery.source() table or view not set");
		if (getView().getViewDef()==null)
			throw new JDOException("SQLQuery.source() table or view definition not set");
		final String tables = getView().getViewDef().getTables();
		if (tables==null || tables.length()==0)
			throw new JDOException("SQLQuery.source() No table or view provided for query");
		try {
			dbms = getView().getConnection().getDataBaseProduct();
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
		final long offset = getRangeFromIncl();
		final long limit = getRangeToExcl()==null ? -1L : getRangeToExcl()-getRangeFromIncl();
		StringBuilder query = new StringBuilder(512);
		query.append("SELECT ");
		query.append(getResult());
		query.append(" FROM ");
		query.append(tables);
		if (getFilter()!=null) {
			if (getFilter().length()>0) {
				query.append(" WHERE ");
				query.append(getFilter());
			}
		}
		if (getGrouping()!=null) {
			if (getGrouping().length()>0) {
				query.append(" GROUP BY ");
				query.append(getGrouping());
			}
		}
		if (getOrdering()!=null) {
			if (getOrdering().length()>0) {
				query.append(" ORDER BY ");
				query.append(getOrdering());
			}
		}
		try {
			if (supportLimitOffset()) {
				if (limit!=-1L)
					query.append(" LIMIT " + String.valueOf(limit));
				if (offset>0)
					query.append(" OFFSET " + String.valueOf(getRangeFromIncl()));
			} else if (dbms==RDBMS.MSSQL.intValue()) {
				if (limit>0)
					query.append(" OPTION (FAST " + String.valueOf(getRangeFromIncl()));			
			}		
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
		return query.toString();
	}

	public PreparedStatement prepareSelect() throws SQLException {
		int ctype = (getRangeFromIncl()==0L ? ResultSet.TYPE_FORWARD_ONLY : ResultSet.TYPE_SCROLL_INSENSITIVE);
		String sql = source();
		if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sql + ")");
		return getView().getConnection().prepareStatement(sql, ctype, ResultSet.CONCUR_READ_ONLY);
	}
	
	@SuppressWarnings("unchecked")
	private <R extends Record> int fetchRowsAsArrays(ResultSet oRSet, RecordSet<R> recordSet, int iMaxRow) throws SQLException {
		int iRetVal = 0;
		boolean bHasNext = true;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin SQLQuery.fetchRowsAsArrays([ResultSet], RecordSet<R>, " + String.valueOf(iMaxRow) + ")");
			DebugFile.incIdent();
		}
		
		R oRow = (R) newRecord();
		final int iColCount = Math.min(oRow.columns().length, oRSet.getMetaData().getColumnCount());
		if (DebugFile.trace) DebugFile.writeln("retval = "+String.valueOf(iRetVal)+" maxrows="+String.valueOf(iMaxRow));			
		while (bHasNext && iRetVal<iMaxRow) {
			iRetVal++;
			for (int iCol=1; iCol<=iColCount; iCol++) {
				Object oFieldValue = oRSet.getObject(iCol);
				if (DebugFile.trace) DebugFile.writeln("ResultSet.getObject("+iCol+") = "+oFieldValue);				
				oRow.put (iCol, oRSet.wasNull() ? null : oFieldValue);
			} // next

			recordSet.add(oRow);

			if (bHasNext = oRSet.next())
				oRow = (R) newRecord();
		} // wend			

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End SQLQuery.fetchRowsAsArrays() : " + iRetVal);
		}

		return iRetVal;
	}

	@SuppressWarnings("unchecked")
	private <R extends Record> int fetchRowsAsMaps(ResultSet oRSet, RecordSet<R> recordSet, int iMaxRow) throws SQLException {
		int iRetVal = 0;
		boolean bHasNext = true;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin SQLQuery.fetchRowsAsMaps([ResultSet], RecordSet<R>, " + String.valueOf(iMaxRow) + ")");
			DebugFile.incIdent();
		}
		
		ResultSetMetaData oMDat = oRSet.getMetaData();
		final int iColCount = oMDat.getColumnCount();
		String[] aColNames = new String[iColCount];
		for (int c=1; c<=oMDat.getColumnCount(); c++) {
			String sColName = oMDat.getColumnName(c);
			aColNames[c-1] = sColName;
		}

		R oRow = (R) newRecord();
		if (DebugFile.trace) DebugFile.writeln("retval = "+String.valueOf(iRetVal)+" maxrows="+String.valueOf(iMaxRow));			
		while (bHasNext && iRetVal<iMaxRow) {
			iRetVal++;
			for (int iCol=1; iCol<=iColCount; iCol++) {
				Object oFieldValue = oRSet.getObject(iCol);
				oRow.put (aColNames[iCol-1], oRSet.wasNull() ? null : oFieldValue);
			} // next

			recordSet.add(oRow);

			if (bHasNext = oRSet.next())
				oRow = (R) newRecord();
		} // wend			

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End SQLQuery.fetchRowsAsMaps() : " + iRetVal);
		}

		return iRetVal;
	}
	
	@SuppressWarnings("unchecked")
	private <R extends Record> int fetchRowsAsSingleColumn(ResultSet oRSet, RecordSet<R> recordSet, int iMaxRow) throws SQLException {
		int iRetVal = 0;
		boolean bHasNext = true;
		String viewName = getView().getAlias();
		if (null==viewName) viewName =  getView().name();
		String colName = oRSet.getMetaData().getColumnName(1);
		
		recordConstructor = (Constructor<? extends Record>) StorageObjectFactory.getConstructor(getResultClass(), new Class<?>[]{String.class.asSubclass(String.class)});
		R oRow = (R) StorageObjectFactory.newRecord(recordConstructor, viewName, colName);

		while (bHasNext && iRetVal<iMaxRow) {
			iRetVal++;
			oRow.setKey(oRSet.getObject(1));

			recordSet.add(oRow);

			if (bHasNext = oRSet.next())
				oRow = (R) (R) StorageObjectFactory.newRecord(recordConstructor, viewName, colName);
		} // wend			
		return iRetVal;
	}

	@SuppressWarnings("unchecked")
	private <R extends Record> RecordSet<R> fetchResultSet (ResultSet oRSet)
			throws NullPointerException, SQLException, SQLFeatureNotSupportedException, ArrayIndexOutOfBoundsException, InstantiationException, IllegalAccessException, NoSuchMethodException
	{
		int iRetVal = 0;
		int iMaxRow = getMaxRows()<0 ? Integer.MAX_VALUE : getMaxRows();
		long lFetchTime = 0;

		if (null==getResultClass())
			throw new NullPointerException("SQLQuery.fetchResultSet() getResultClass() cannot be null");
			
		if (DebugFile.trace) {
			DebugFile.writeln("Begin SQLQuery.fetchResultSet([ResultSet], " + String.valueOf(iMaxRow) + ", " + String.valueOf(getRangeFromIncl()) + ")");
			DebugFile.incIdent();
		}
		
		RecordSet<R> recordSet = StorageObjectFactory.newRecordSetOf(getResultClass(), new Integer(iMaxRow));
		
		if (DebugFile.trace)
			lFetchTime = System.currentTimeMillis();

		boolean bHasNext = true;
		
		if (supportLimitOffset()) {
			bHasNext = oRSet.next();
		} else {
			if (0l==getRangeFromIncl())
				bHasNext = oRSet.next();
			else
				bHasNext = oRSet.relative (getRangeFromIncl().intValue()+1);
		}

		if (DebugFile.trace) DebugFile.writeln("has next " + String.valueOf(bHasNext));

		if (bHasNext) {
			if ((ArrayRecordJava!=null && ArrayRecordJava.isAssignableFrom(getResultClass())) ||
				(ArrayRecordScala!=null && ArrayRecordScala.isAssignableFrom(getResultClass())))
				iRetVal = fetchRowsAsArrays(oRSet, recordSet, iMaxRow);
			else if (SingleColumnRecord.class.isAssignableFrom(getResultClass()))
				iRetVal = fetchRowsAsSingleColumn(oRSet, recordSet, iMaxRow);				
			else
				iRetVal = fetchRowsAsMaps(oRSet, recordSet, iMaxRow);				
		} // fi

		if (0==iRetVal || iRetVal<iMaxRow) {
			endOfFetch = true;
			if (DebugFile.trace) DebugFile.writeln("readed " + String.valueOf(iRetVal) + " rows eof() = true");
		}
		else {
			endOfFetch = !bHasNext;

			if (DebugFile.trace) DebugFile.writeln("readed max " + String.valueOf(iMaxRow) + " rows eof() = " + String.valueOf(endOfFetch));
		}

		if (DebugFile.trace) {
			DebugFile.writeln("fetching done in " + String.valueOf(System.currentTimeMillis()-lFetchTime) + " ms");
			DebugFile.decIdent();
			DebugFile.writeln("End SQLQuery.fetchResultSet() : " + String.valueOf(iRetVal));
		}

		return recordSet;
	} // fetchResultSet

	private void setFetchSize(ResultSet oRSet) throws SQLException {

		int fetch = getMaxRows();

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCQuery.setFetchSize()");
			DebugFile.incIdent();
		}

		if (getView().getConnection().getDataBaseProduct()==RDBMS.POSTGRESQL.intValue()) {
			// PostgreSQL does not support setFetchSize()
			fetch = 1;
		} else {
			try {
				if (0!=fetch)
					oRSet.setFetchSize (fetch);
				else
					fetch = oRSet.getFetchSize();
			}
			catch (SQLException e) {
				if (DebugFile.trace) DebugFile.writeln(e.getMessage());
				fetch = 1;
			}
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCQuery.setFetchSize() : " + fetch);
		}
	} // setFetchSize

	/**
	 * <p>This function will always return <b>null</b>
	 */
	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}

}
