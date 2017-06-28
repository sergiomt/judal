package org.judal.jdbc;

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

import javax.jdo.JDOException;

import org.judal.storage.query.sql.SQLQuery;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Predicate;

public class JDBCRelationalView extends JDBCIndexableView implements RelationalView {

	public JDBCRelationalView(JDBCTableDataSource dataSource, Record recordInstance) throws JDOException {
		super(dataSource, recordInstance);
	}

	public JDBCRelationalView(JDBCTableDataSource dataSource, SQLTableDef tableDef, Class<? extends Record> recClass) throws JDOException {
		super(dataSource, tableDef, recClass);
	}	
	
	@Override
	public Long count(Predicate filterPredicate) {
		Long retval;
		if (null==filterPredicate) {
			retval = count(null,null);
		} else {
			SQLQuery qry = newQuery();
			qry.setFilter(filterPredicate);
			qry.setResult("COUNT(*)");
		
			PreparedStatement stmt = null;
			ResultSet rset = null;
			try {
				stmt = qry.prepareSelect();
				qry.setParameters(stmt);
				rset = stmt.executeQuery();
				rset.next();
				retval = rset.getLong(1);
				rset.close();
				rset = null;
				stmt.close();
				stmt = null;
			} catch (SQLException sqle) {
				throw new JDOException(sqle.getMessage(), sqle);
			} finally {
				try { if (rset!=null) rset.close(); } catch (Exception ignore) { }
				try { if (stmt!=null) stmt.close(); } catch (Exception ignore) { }
			}
		}
		return retval;
	}

	public Object aggregate(String sqlFunc, String result, Predicate filterPredicate) throws JDOException {
		Object retval;
		PreparedStatement stmt = null;
		ResultSet rset = null;
		try {
			if (filterPredicate==null) {
				stmt = getConnection().prepareStatement("SELECT "+sqlFunc+"("+result+") FROM "+getTableDef().getTable());
			} else {
				SQLQuery qry = newQuery();
				qry.setFilter(filterPredicate);
				qry.setResult(sqlFunc+"("+result+")");
				stmt = qry.prepareSelect();				
				qry.setParameters(stmt);
			}
			rset = stmt.executeQuery();
			rset.next();
			retval = rset.getObject(1);
			rset.close();
			rset = null;
			stmt.close();
			stmt = null;
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		} finally {
			try { if (rset!=null) rset.close(); } catch (Exception ignore) { }
			try { if (stmt!=null) stmt.close(); } catch (Exception ignore) { }
		}
		return retval;
		
	}

	@Override
	public Number sum(String result, Predicate filterPredicate) {
		return (Number) aggregate("SUM", result, filterPredicate) ;
	}

	@Override
	public Number avg(String result, Predicate filterPredicate) {
		return (Number) aggregate("AVG", result, filterPredicate) ;
	}

	@Override
	public Object max(String result, Predicate filterPredicate) {
		return aggregate("MAX", result, filterPredicate) ;
	}

	@Override
	public Object min(String result, Predicate filterPredicate) {
		return aggregate("MIN", result, filterPredicate) ;
	}

	@Override
	public Predicate newPredicate() throws JDOException {
		return newQuery().newPredicate();
	}

	@Override
	public SQLQuery newQuery() throws JDOException {
		return new SQLQuery(this);
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(AbstractQuery qry)
		throws JDOException {
		return super.fetchQuery(qry);	
	} // fetch

}
