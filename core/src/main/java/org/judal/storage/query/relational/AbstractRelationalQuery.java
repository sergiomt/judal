package org.judal.storage.query.relational;

import java.lang.reflect.InvocationTargetException;

import javax.jdo.FetchGroup;
import javax.jdo.JDOUserException;

import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Part;
import org.judal.storage.query.Predicate;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

public abstract class AbstractRelationalQuery<R extends Record> implements Cloneable, AutoCloseable {

	  protected RelationalView viw;
	  protected AbstractQuery qry;
	  protected Predicate prd;

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

	  public AbstractRelationalQuery<R> addPart(Part term) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.addPart(term);
		  return this;
	  }

	  public AbstractRelationalQuery<R> or(String column, String operator, Object param) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.or(column, operator, param);
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
	  
}
