package org.judal.storage.query.relational;

import java.lang.reflect.InvocationTargetException;

import javax.jdo.JDOException;

import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;

public class RelationalQuery<R extends Record> extends AbstractRelationalQuery<R> {
	  
	  public RelationalQuery(RelationalDataSource dts, Class<R> recClass) throws JDOException {
	  	R rec;
		try {
			rec = recClass.getConstructor(TableDataSource.class).newInstance(dts);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new JDOException(e.getMessage(), e);
		}
		viw = dts.openRelationalView(rec);
	  	qry = viw.newQuery();
	  	prd = qry.newPredicate();
	  }
	
}
