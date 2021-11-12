package org.judal.jdbc;

/*
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.judal.jdbc.metadata.SQLViewDef;

import javax.jdo.JDOException;
import javax.jdo.Query;

import org.judal.storage.query.sql.SQLQuery;
import org.judal.storage.query.sql.SQLPredicate;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Predicate;

/**
 * <p>JDBC relational view.</p>
 * Instances of this class are used to fetch or load records from a relational view accessed through a JDBC data source.
 * @author Sergio Montoro Ten
 * @version 1.0
 */public class JDBCRelationalView extends JDBCIndexableView implements RelationalView {


	 /**
	  * <p>Constructor.</p>
	  * When using this constructor, the table with name as returned by recordInstance.getTableName() must exist in the data source schema metadata.
	  * @param dataSource JDBCTableDataSource
	  * @param recordInstance Record Instance of the Record implementation that will be used to fetch, load, store and delete records from the RDBMS table.
	  * @throws JDOException
	  */	 
	 public JDBCRelationalView(JDBCTableDataSource dataSource, Record recordInstance) throws JDOException {
		 super(dataSource, recordInstance);
	 }

	 /**
	  * <p>Constructor.</p>
	  * @param dataSource JDBCTableDataSource
	  * @param viewDef SQLViewDef
	  * @param recClass Class&lt;? extends Record&gt; Class of the Record implementation that will be used to fetch, load, store and delete records from the RDBMS table.
	  * @throws JDOException
	  */
	 public JDBCRelationalView(JDBCTableDataSource dataSource, SQLViewDef viewDef, Class<? extends Record> recClass) throws JDOException {
		 super(dataSource, viewDef, recClass);
	 }	

	 /**
	  * <p>Count records matching a Predicate.</p>
	  * @param filterPredicate Predicate
	  * @return Long
	  * @throws JDOException
	  */
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

	 /**
	  * <p>Apply an SQL aggregate function to the results of a filter predicate.</p> 
	  * @param sqlFunc String SQL function like SUM, AVG, &hellip; 
	  * @param result String Column name or expression to be aggregated
	  * @param filterPredicate Predicate
	  * @return Object The return type depends on the column type and the function applied
	  * @throws JDOException
	  */
	 public Object aggregate(String sqlFunc, String result, Predicate filterPredicate) throws JDOException {
		 Object retval;
		 PreparedStatement stmt = null;
		 ResultSet rset = null;
		 try {
			 if (filterPredicate==null) {
				 stmt = getConnection().prepareStatement("SELECT "+sqlFunc+"("+result+") FROM "+getViewDef().getTable());
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

	 /**
	  * <p>Get sum of values in a column for records matching a Predicate.</p>
	  * @param result String Column name
	  * @param filterPredicate Predicate
	  * @return Number
	  * @throws JDOException
	  */
	 @Override
	 public Number sum(String result, Predicate filterPredicate) {
		 return (Number) aggregate("SUM", result, filterPredicate) ;
	 }

	 /**
	  * <p>Get average of values in a column for records matching a Predicate.</p>
	  * @param result String Column name
	  * @param filterPredicate Predicate
	  * @return Number
	  * @throws JDOException
	  */
	 @Override
	 public Number avg(String result, Predicate filterPredicate) {
		 return (Number) aggregate("AVG", result, filterPredicate) ;
	 }

	 /**
	  * <p>Get maximum value in a column for records matching a Predicate.</p>
	  * @param result String Column name
	  * @param filterPredicate Predicate
	  * @return Object
	  * @throws JDOException
	  */	@Override
	  public Object max(String result, Predicate filterPredicate) {
		  return aggregate("MAX", result, filterPredicate) ;
	  }

	  /**
	   * <p>Get minimum value in a column for records matching a Predicate.</p>
	   * @param result String Column name
	   * @param filterPredicate Predicate
	   * @return Object
	   * @throws JDOException
	   */
	  @Override
	  public Object min(String result, Predicate filterPredicate) {
		  return aggregate("MIN", result, filterPredicate) ;
	  }

	  /**
	   * <p>Create new query predicate with Connective.NONE.</p>
	   * @return Predicate
	   * @throws JDOException
	   */
	  @Override
	  public Predicate newPredicate() throws JDOException {
		  SQLPredicate predicate = newQuery().newPredicate();
		  predicate.setSQLFunctions(getDataSource().Functions);
		  return predicate;
	  }

	  /**
	   * <p>Create new Query.</p>
	   * @return SQLQuery
	   * @throws JDOException
	   */
	  @Override
	  public SQLQuery newQuery() throws JDOException {
		  return new SQLQuery(this);
	  }

	  /**
	   * <p>Fetch records which match a given query.</p>
	   * @param qry SQLQuery
	   * @return RecordSet&lt;&lt;? extends Record&gt;&gt;
	   * @throws JDOException
	   */
	  @Override
	  public <R extends Record> RecordSet<R> fetch(Query qry)
			  throws JDOException {
		  return super.fetchQuery((AbstractQuery) qry);
	  } // fetch

	  /**
	   * <p>Fetch first record which matches a given query.</p>
	   * @param query SQLQuery
	   * @return RecordSet&lt;&lt;? extends Record&gt;&gt; or null if no record was returned by the query
	   * @throws JDOException
	   */
	@Override
	public <R extends Record> R fetchFirst(Query query) throws JDOException {
		AbstractQuery querya = (AbstractQuery) query;
		AbstractQuery query1;
		if (querya.getRangeFromIncl()!=0l || querya.getRangeToExcl()!=1l) {
			query1 = querya.clone();
			query.setRange(0,1);			
		} else {
			query1 = querya;
		}
		RecordSet<R> rst = super.fetchQuery(query1);
		if (!rst.isEmpty())
			return rst.get(0);
		else
			return null;
	}

 }
