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

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;

import org.judal.jdbc.JDBCRelationalView;
import org.judal.jdbc.RDBMS;
import org.judal.storage.StorageObjectFactory;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Connective;
import org.judal.storage.table.ArrayListRecordSet;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

import com.knowgate.debug.DebugFile;
import com.knowgate.debug.StackTraceUtil;

public class SQLQuery extends AbstractQuery {

	private static final long serialVersionUID = 1L;
	
	private Constructor<? extends Record> recordConstructor;
	private Object[] constructorParameters;

	public SQLQuery(IndexableView view) throws JDOException {
		setCandidates(view);
		setRange(0, Integer.MAX_VALUE);
		recordConstructor = null;
	}

	private JDBCRelationalView getView() {
		return (JDBCRelationalView) getCandidates();
	}
	
	@Override
	public String getResult() {
		final String retval = getResult();
		return retval==null ? "*" : retval;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Record newRecord() {
		if (null==recordConstructor) {
			recordConstructor = (Constructor<? extends Record>) StorageObjectFactory.getConstructor(getResultClass(), new Class<?>[]{getView().getDataSource().getClass(), getView().getTableDef().getClass()});
			try {
				constructorParameters = StorageObjectFactory.filterParameters(recordConstructor.getParameters(), new Object[]{getView().getDataSource(), getView().getTableDef()});
			} catch (InstantiationException e) {
				throw new JDOException(e.getMessage(), e);
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

	public void setParameters(PreparedStatement stmt) throws SQLException {		
		int p = 1;
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
			} else {
				if (DebugFile.trace) DebugFile.writeln("PreparedStatement.setObject("+String.valueOf(p)+","+oParam.toString()+")");
				stmt.setObject(p++, oParam);
			} // fi
		} // next
	}

	@Override
	public RecordSet<? extends Record> execute() {
		PreparedStatement stmt = null;
		ResultSet rset = null;
		RecordSet<? extends Record> retval;

		long qtime = 0;

		if (DebugFile.trace)
			DebugFile.writeln("Begin JDBCQuery.execute(" + getRangeFromIncl() + "," + getRangeToExcl() + ")");

		if (getRangeFromIncl()<0l)
			throw new IllegalArgumentException("row offset must be equal to or greater than zero");

		if (DebugFile.trace)
			DebugFile.incIdent();

		try {
			stmt = prepareSelect();

			setParameters(stmt);

			if (DebugFile.trace) {
				DebugFile.writeln("PreparedStatement.executeQuery()");
				qtime = System.currentTimeMillis();
			}

			rset = stmt.executeQuery();

			if (DebugFile.trace) {
				DebugFile.writeln("query executed in " + String.valueOf(System.currentTimeMillis()-qtime) + " ms");
			}

			setFetchSize(rset);

			retval = fetchResultSet(rset);

			if (DebugFile.trace) DebugFile.writeln("ResultSet.close()");

			rset.close();
			rset = null;

			if (DebugFile.trace) DebugFile.writeln("PreparedStatement.close()");

			stmt.close();
			stmt = null;
		}
		catch (SQLException sqle) {
			try { 
				if (DebugFile.trace) DebugFile.writeln("SQLException "+sqle.getMessage()+"\n"+StackTraceUtil.getStackTrace(sqle));
			} catch (java.io.IOException ignore) {}
			try { if (null!=rset) rset.close();
			} catch (Exception logit) { if (DebugFile.trace) DebugFile.writeln(logit.getClass().getName()+" "+logit.getMessage()); }
			try { if (null!=stmt) stmt.close();
			} catch (Exception logit) { if (DebugFile.trace) DebugFile.writeln(logit.getClass().getName()+" "+logit.getMessage()); }
			if (getParameters().length==0)
				throw new JDOException(sqle.getSQLState()+" "+sqle.getMessage()+" zero parameters set", sqle);
			else {
				throw new JDOException(sqle.getSQLState()+" "+sqle.getMessage(), sqle);
			}
		}
		catch (IllegalAccessException | InstantiationException | ArrayIndexOutOfBoundsException | NullPointerException xcpt) {
			try { 
				if (DebugFile.trace) DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage()+"\n"+StackTraceUtil.getStackTrace(xcpt));
			} catch (java.io.IOException ignore) {}
			try { if (null!=rset) rset.close();
			} catch (Exception logit) { if (DebugFile.trace) DebugFile.writeln(logit.getClass().getName()+" "+logit.getMessage()); }
			try { if (null!=stmt) stmt.close();
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
			DebugFile.writeln("End JDBCIndexableView.fetch() : "+String.valueOf(retval.size()));
		}

		return retval;
	} // execute
	
	@Override
	public String source() throws JDOException {
		int dbms;
		try {
			dbms = getView().getConnection().getDataBaseProduct();
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
		final long offset = getRangeFromIncl();
		final long limit = getRangeToExcl()==null ? -1L : getRangeToExcl()-getRangeFromIncl();
		StringBuffer query = new StringBuffer(512);
		query.append("SELECT ");
		query.append(getResult());
		query.append(" FROM ");
		query.append(getView().getTableDef().getTable());
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

	private PreparedStatement prepareSelect() throws SQLException {
		int ctype = (getRangeFromIncl()==0L ? ResultSet.TYPE_FORWARD_ONLY : ResultSet.TYPE_SCROLL_INSENSITIVE);
		String sql = source();
		if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement(" + sql + ")");
		return getView().getConnection().prepareStatement(sql, ctype, ResultSet.CONCUR_READ_ONLY);
	}
	
	private <R extends Record> int fetchRowsAsArrays(Constructor<? extends Record> recordConstructor, ResultSet oRSet, ArrayListRecordSet<R> recordSet, int iMaxRow) throws SQLException {
		int iRetVal = 0;
		boolean bHasNext = true;
		
		R oRow = (R) newRecord();
		final int iColCount = oRow.columns().length;
		if (DebugFile.trace) DebugFile.writeln("retval = "+String.valueOf(iRetVal)+" maxrows="+String.valueOf(iMaxRow));			
		while (bHasNext && iRetVal<iMaxRow) {
			iRetVal++;
			for (int iCol=1; iCol<=iColCount; iCol++) {
				Object oFieldValue = oRSet.getObject(iCol);
				oRow.put (iCol, oRSet.wasNull() ? null : oFieldValue);
			} // next

			recordSet.add(oRow);

			if (bHasNext = oRSet.next())
				oRow = (R) newRecord();
			if (DebugFile.trace) DebugFile.writeln("retval = "+String.valueOf(iRetVal)+" has next="+String.valueOf(bHasNext));
		} // wend			
		return iRetVal;
	}

	private <R extends Record> int fetchRowsAsMaps(Constructor<? extends Record> recordConstructor, ResultSet oRSet, ArrayListRecordSet<R> recordSet, int iMaxRow) throws SQLException {
		int iRetVal = 0;
		boolean bHasNext = true;
		
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
			if (DebugFile.trace) DebugFile.writeln("retval = "+String.valueOf(iRetVal)+" has next="+String.valueOf(bHasNext));
		} // wend			
		return iRetVal;
	}
	
	private <R extends Record> ArrayListRecordSet<R> fetchResultSet (ResultSet oRSet)
			throws SQLException, SQLFeatureNotSupportedException, ArrayIndexOutOfBoundsException, InstantiationException, IllegalAccessException
	{
		int iRetVal = 0;
		int iMaxRow = getMaxRows()<0 ? Integer.MAX_VALUE : getMaxRows();
		long lFetchTime = 0;
		ArrayListRecordSet<R> recordSet = new ArrayListRecordSet<R>(getResultClass(), iMaxRow);

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCQuery.fetchResultSet([ResultSet], " + String.valueOf(iMaxRow) + ", " + String.valueOf(getRangeFromIncl()) + ")");
			DebugFile.incIdent();
			lFetchTime = System.currentTimeMillis();
		}

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
			if (newRecord().getClass().getName().endsWith("ArrayRecord"))
				iRetVal = fetchRowsAsArrays(recordConstructor, oRSet, recordSet, iMaxRow);
			else
				iRetVal = fetchRowsAsMaps(recordConstructor, oRSet, recordSet, iMaxRow);				
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
			DebugFile.writeln("End JDBCQuery.fetchResultSet() : " + String.valueOf(iRetVal));
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

	@Override
	public FetchPlan getFetchPlan() {
		// TODO Auto-generated method stub
		return null;
	}

}
