package org.judal.storage.query.relational;

import java.lang.reflect.InvocationTargetException;

import javax.jdo.JDOUserException;

import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Part;
import org.judal.storage.query.Predicate;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

public abstract class AbstractRelationalQuery<R extends Record> implements AutoCloseable {

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
	  
	  public AbstractRelationalQuery<R> and(String column, String operator, Object param) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, param);
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

	  public RecordSet<R> fetch() {
	      qry.setFilter(prd);
		  return viw.fetch(qry);
	  }

	  @Override
	  public void close() throws Exception {
		  if (viw!=null) {
			  viw.close();
			  viw = null;
		  }
	  }
	  
}
