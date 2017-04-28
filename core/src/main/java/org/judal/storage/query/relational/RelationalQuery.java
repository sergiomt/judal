package org.judal.storage.query.relational;

import java.lang.reflect.InvocationTargetException;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;

import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Predicate;

public class RelationalQuery<R extends Record> implements AutoCloseable {

	  private RelationalView viw;
	  private AbstractQuery qry;
	  private Predicate prd;
	  
	  public RelationalQuery(RelationalDataSource dts, Class<R> recClass) throws JDOException {
	  	R rec;
		try {
			rec = recClass.getConstructor(TableDataSource.class).newInstance(dts);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new JDOException(e.getMessage(), e);
		}
		this.viw = dts.openRelationalView(rec);
	  	qry = viw.newQuery();
	  	prd = qry.newPredicate();
	  }

	  public RelationalQuery<R> setRange(long fromInc, long toExc) {
		  qry.setRange(fromInc, toExc);
		  return this;
	  }

	  public RelationalQuery<R> setOrdering(String orderingExpression) {
		  qry.setOrdering(orderingExpression);
		  return this;
	  }
	  
	  public RelationalQuery<R> setResult(String commaDelimitedColumnNames) {
		qry.setResult(commaDelimitedColumnNames);
		return this;
	  }

	  public RelationalQuery<R> setResult(Iterable<String> columnNames) {
		qry.setResult(columnNames);
		return this;
	  }

	  public RelationalQuery<R> and(String column, String operator, Object param) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		  prd.and(column, operator, param);
		  return this;
	  }
	  
	  public RelationalQuery<R> or(String column, String operator, Object param) throws JDOUserException, UnsupportedOperationException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
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
