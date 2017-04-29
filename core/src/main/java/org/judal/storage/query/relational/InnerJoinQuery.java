package org.judal.storage.query.relational;

import java.util.AbstractMap.SimpleImmutableEntry;

import java.lang.reflect.InvocationTargetException;

import javax.jdo.JDOException;

import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;

public class InnerJoinQuery<R extends Record, T extends Record> extends AbstractRelationalQuery<R> {

	  public InnerJoinQuery(RelationalDataSource dts, Class<R> recClass1, Class<T> recClass2, String column1, String column2) throws JDOException {
		  	R rec1;
		  	T rec2;
			try {
				rec1 = recClass1.getConstructor(TableDataSource.class).newInstance(dts);
				rec2 = recClass2.getConstructor(TableDataSource.class).newInstance(dts);
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
					| NoSuchMethodException | SecurityException e) {
				throw new JDOException(e.getMessage(), e);
			}
			viw = dts.openInnerJoinView(rec1, rec2.getTableName(), new SimpleImmutableEntry<String,String>(column1, column2));
		  	qry = viw.newQuery();
		  	prd = qry.newPredicate();
		  }

}
