package org.judal.storage.query.relational;

import java.lang.reflect.InvocationTargetException;

import javax.jdo.JDOUserException;

import org.judal.metadata.NameAlias;
import org.judal.storage.Param;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Connective;
import org.judal.storage.query.Part;
import org.judal.storage.query.Predicate;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

public abstract class AbstractRelationalQuery<R extends Record> implements Cloneable, AutoCloseable {

	  protected RelationalView viw;
	  protected AbstractQuery qry;
	  protected Predicate prd;

	  public AbstractRelationalQuery<R> setFilter(String filterExpression) {
		qry.setFilter(filterExpression);
		return this;
	  }
	  
	  public AbstractRelationalQuery<R> setRange(long fromInc, long toExc) {
		  qry.setRange(fromInc, toExc);
		  return this;
	  }

	  public AbstractRelationalQuery<R> setOrdering(String orderingExpression) {
		  qry.setOrdering(orderingExpression);
		  return this;
	  }
	  
	  public AbstractRelationalQuery<R> setResult(String commaDelimitedColumnNames) {
		qry.setResult(commaDelimitedColumnNames);
		return this;
	  }

	  public AbstractRelationalQuery<R> setResult(Iterable<String> columnNames) {
		  qry.setResult(columnNames);
		return this;
	  }
	  	  
	  public AbstractRelationalQuery<R> setResultClass(Class<?> resultClass) {
		  qry.setResultClass(resultClass);
		return this;
	  }

	  public AbstractRelationalQuery<R> setClass(Class<?> candidateClass) {
		  qry.setClass(candidateClass);
		return this;
	  }

	  public AbstractRelationalQuery<R> and(String column, String operator, Object param) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, param);
		  return this;
	  }

	  public AbstractRelationalQuery<R> and(String column, String operator, String nestedTable, Part subselect) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, nestedTable, subselect);
		  return this;
	  }

	  public AbstractRelationalQuery<R> and(String column, String operator, NameAlias aliasedNestedTable, Part subselect) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, aliasedNestedTable, subselect);
		  return this;
	  }
	  
	  public AbstractRelationalQuery<R> and(String column, String operator, String nestedTable, String nestedColumnName, Part subselect) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, nestedTable, nestedColumnName, subselect);
		  return this;
	  }

	  public AbstractRelationalQuery<R> and(String column, String operator, NameAlias aliasedNestedTable, String nestedColumnName, Part subselect) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, aliasedNestedTable, nestedColumnName, subselect);
		  return this;
	  }
	  
	  public AbstractRelationalQuery<R> and(String column, String operator, String nestedTable, String nestedColumnName) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, nestedTable, nestedColumnName, (Part) null);
		  return this;
	  }

	  public AbstractRelationalQuery<R> and(String column, String operator, NameAlias aliasedNestedTable, String nestedColumnName) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, aliasedNestedTable, nestedColumnName, (Part) null);
		  return this;
	  }
	  
	  public AbstractRelationalQuery<R> and(String column, String operator, java.util.Date dateFrom, java.util.Date dateTo) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, new java.util.Date[]{dateFrom,dateTo});
		  return this;
	  }
	  
	  public AbstractRelationalQuery<R> and(String column, String operator, java.util.Calendar dateFrom, java.util.Calendar dateTo) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, new java.util.Calendar[]{dateFrom,dateTo});
		  return this;
	  }

	  public AbstractRelationalQuery<R> and(String column, String operator, java.sql.Date dateFrom, java.sql.Date dateTo) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, new java.sql.Date[]{dateFrom,dateTo});
		  return this;
	  }

	  public AbstractRelationalQuery<R> and(String column, String operator, java.sql.Timestamp dateFrom, java.sql.Timestamp dateTo) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, new java.sql.Timestamp[]{dateFrom,dateTo});
		  return this;
	  }

	  public AbstractRelationalQuery<R> addPart(Part term) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.addPart(term);
		  return this;
	  }

	  public AbstractRelationalQuery<R> or(String column, String operator, Object param) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.or(column, operator, param);
		  return this;
	  }

	  public AbstractRelationalQuery<R> or(String column, String operator, String nestedTable, String nestedColumnName, Part subselect) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.or(column, operator, nestedTable, nestedColumnName, subselect);
		  return this;
	  }

	  public AbstractRelationalQuery<R> or(String column, String operator, NameAlias aliasedNestedTable, String nestedColumnName, Part subselect) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.or(column, operator, aliasedNestedTable, nestedColumnName, subselect);
		  return this;
	  }
	  
	  public AbstractRelationalQuery<R> or(String column, String operator, String nestedTable, String nestedColumnName) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.or(column, operator, nestedTable, nestedColumnName, (Part) null);
		  return this;
	  }

	  public AbstractRelationalQuery<R> or(String column, String operator, NameAlias aliasedNestedTable, String nestedColumnName) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.or(column, operator, aliasedNestedTable, nestedColumnName, (Part) null);
		  return this;
	  }
	  
	  public AbstractRelationalQuery<R> or(String column, String operator, java.util.Date dateFrom, java.util.Date dateTo) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.or(column, operator, new java.util.Date[]{dateFrom,dateTo});
		  return this;
	  }
	  
	  public AbstractRelationalQuery<R> or(String column, String operator, java.util.Calendar dateFrom, java.util.Calendar dateTo) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.or(column, operator, new java.util.Calendar[]{dateFrom,dateTo});
		  return this;
	  }

	  public AbstractRelationalQuery<R> or(String column, String operator, java.sql.Date dateFrom, java.sql.Date dateTo) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.or(column, operator, new java.sql.Date[]{dateFrom,dateTo});
		  return this;
	  }

	  public AbstractRelationalQuery<R> or(String column, String operator, java.sql.Timestamp dateFrom, java.sql.Timestamp dateTo) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.or(column, operator, new java.sql.Timestamp[]{dateFrom,dateTo});
		  return this;
	  }
	  
	  public abstract AbstractRelationalQuery<R> clone();

	  public int count() {
		  AbstractQuery counter = qry.clone();
		  counter.setResult("COUNT(*) AS ROWS_COUNT");
		  counter.setRange(0, 1);
		  return viw.fetch(counter).get(0).getInt("ROWS_COUNT");
	  }

	  public RecordSet<R> fetch() {
	      qry.setFilter(prd);
		  return viw.fetch(qry);
	  }
	  
	  public boolean eof() {
		  return qry.eof();
	  }

	  protected void clone(AbstractRelationalQuery<R> source) {
		  viw = source.viw;
		  qry = source.qry.clone();
		  prd = source.prd.clone();		  
	  }

	  @Override
	  public void close() throws Exception {
		  if (viw!=null) {
			  viw.close();
			  viw = null;
		  }
	  }
	  
	  public Predicate newPredicate() {
		  return qry.newPredicate();
	  }

	  public Predicate newPredicate(Connective logicalConnective) {
		  return qry.newPredicate(logicalConnective);
	  }

	  public Predicate newPredicate(Connective logicalConnective, String operator, Param[] filterParameters) {
		  return qry.newPredicate(logicalConnective, operator, filterParameters);
	  }
	  
}
