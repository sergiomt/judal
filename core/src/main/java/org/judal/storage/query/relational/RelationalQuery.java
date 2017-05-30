package org.judal.storage.query.relational;

import java.lang.reflect.InvocationTargetException;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.StorageObjectFactory;
import org.judal.storage.table.Record;
import org.judal.storage.relational.RelationalDataSource;

public class RelationalQuery<R extends Record> extends AbstractRelationalQuery<R> {

	public RelationalQuery(Class<R> recClass) throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource(), recClass);
	}

	public RelationalQuery(Class<R> recClass, String alias) throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource(), recClass, alias);
	}

	public RelationalQuery(RelationalDataSource dts, Class<R> recClass) throws JDOException {
		R rec;
		try {
			rec = StorageObjectFactory.newRecord(recClass, dts);
		} catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
			throw new JDOException(e.getClass().getName() + "" + e.getMessage(), e);
		}
		viw = dts.openRelationalView(rec);
		qry = viw.newQuery();
		prd = qry.newPredicate();
	}

	public RelationalQuery(RelationalDataSource dts, Class<R> recClass, String alias) throws JDOException {
		this(dts, recClass);
		try {
			viw.getClass().getMethod("setAlias", String.class).invoke(viw, alias);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
				| SecurityException e) {
			throw new JDOException(e.getClass().getName() + "" + e.getMessage(), e);
		}
	}

	public RelationalQuery(Record rec) throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource(), rec);
	}

	public RelationalQuery(Record rec, String alias) throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource(), rec, alias);
	}
	
	public RelationalQuery(RelationalDataSource dts, Record rec) throws JDOException {
		viw = dts.openRelationalView(rec);
		qry = viw.newQuery();
		prd = qry.newPredicate();
	}
	
	public RelationalQuery(RelationalDataSource dts, Record rec, String alias) throws JDOException {
		viw = dts.openRelationalView(rec);
		try {
			viw.getClass().getMethod("setAlias", String.class).invoke(viw, alias);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
				| SecurityException e) {
			throw new JDOException(e.getClass().getName() + "" + e.getMessage(), e);
		}
		qry = viw.newQuery();
		prd = qry.newPredicate();
	}

	private RelationalQuery() {
	}

	@Override
	public RelationalQuery<R> clone() {
		RelationalQuery<R> theClone = new RelationalQuery<>();
		theClone.clone(this);
		return theClone;
	}

}
