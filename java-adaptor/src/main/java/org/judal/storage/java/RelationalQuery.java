package org.judal.storage.java;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.query.relational.AbstractRelationalQuery;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.relational.RelationalDataSource;

public class RelationalQuery<R extends Record> extends AbstractRelationalQuery<R> {

	public RelationalQuery(Class<R> recClass) throws JDOException {
		super(recClass);
	}

	public RelationalQuery(Class<R> recClass, String alias) throws JDOException {
		this(EngineFactory.getDefaultRelationalDataSource(), recClass, alias);
	}

	public RelationalQuery(RelationalDataSource dts, Class<R> recClass) throws JDOException {
		super(dts, recClass);
	}

	public RelationalQuery(RelationalDataSource dts, Class<R> recClass, String alias) throws JDOException {
		super(dts, recClass, alias);
	}

	public RelationalQuery(R rec) throws JDOException {
		super(rec);
	}

	public RelationalQuery(R rec, String alias) throws JDOException {
		super(rec, alias);
	}
	
	public RelationalQuery(RelationalDataSource dts, R rec) throws JDOException {
		super(dts, rec);
	}
	
	public RelationalQuery(RelationalDataSource dts, R rec, String alias) throws JDOException {
		super(dts, rec, alias);
	}

	private RelationalQuery() {
	}

	@Override
	public RelationalQuery<R> clone() {
		RelationalQuery<R> theClone = new RelationalQuery<>();
		theClone.clone(this);
		return theClone;
	}

	@Override
	public RecordSet<R> fetch() {
		qry.setFilter(prd);
		return viw.fetch(qry);
	}
	
}
