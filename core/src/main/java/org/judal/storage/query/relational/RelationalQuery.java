package org.judal.storage.query.relational;

import java.lang.reflect.InvocationTargetException;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.StorageObjectFactory;
import org.judal.storage.table.Record;
import org.judal.storage.relational.RelationalDataSource;

public class RelationalQuery<R extends Record> extends AbstractRelationalQuery<R> {
	  
	  public RelationalQuery(Class<R> recClass) throws JDOException {
		  this((RelationalDataSource) EngineFactory.DefaultThreadDataSource.get(), recClass);
	  }

	  public RelationalQuery(RelationalDataSource dts, Class<R> recClass) throws JDOException {
	  	R rec;
		try {
			rec = StorageObjectFactory.newRecord(recClass, dts);
		} catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
			throw new JDOException(e.getClass().getName()+""+e.getMessage(), e);
		}
		viw = dts.openRelationalView(rec);
	  	qry = viw.newQuery();
	  	prd = qry.newPredicate();
	  }

	  public RelationalQuery(RelationalDataSource dts, Class<R> recClass, String alias) throws JDOException {
		  this(dts, recClass);
		  try {
			viw.getClass().getMethod("setAlias", String.class).invoke(viw, alias);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
			throw new JDOException(e.getClass().getName()+""+e.getMessage(), e);
		}
	  }

	  private RelationalQuery() { }

	  @Override
	  public RelationalQuery<R> clone() {
		  RelationalQuery<R> theClone = new RelationalQuery<>();
		  theClone.clone(this);
		  return theClone;
	  }	  

}
