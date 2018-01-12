package org.judal.storage.java;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

import javax.jdo.JDOException;

import org.judal.storage.EngineFactory;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.relational.AbstractRelationalQuery;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

import com.knowgate.tuples.Pair;

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

	@Override
	public R fetchFirst() {
		RecordSet<R> rst;
		AbstractQuery qry1;
		qry.setFilter(prd);
		if (qry.getRangeFromIncl()==0l && qry.getRangeToExcl()==1l) {
			qry1 = qry;
		} else {
			qry1 = qry.clone();
			qry.setRange(0l, 1l);
		}			
		rst = viw.fetch(qry1);
		return rst.isEmpty() ? null : rst.get(0);
	}

	@SuppressWarnings("unchecked")
	public RecordSet<R> fetchWithArray(Entry<String,Object>... params) {
		if (params!=null) {
			final int nparams = params.length;
			StringBuilder paramList = new StringBuilder();
			Object[] paramValues = new Object[nparams];
			for (int p=0; p<nparams; p++) {
				paramList.append(p==0 ? "" : ",").append(params[p].getKey());
				paramValues[p] = params[p].getValue();
			}
			qry.declareParameters(paramList.toString());
			return (RecordSet<R>) qry.executeWithArray(paramValues);
		} else {
			return (RecordSet<R>) qry.execute();
		}
	}

	@SuppressWarnings("unchecked")
	public RecordSet<R> fetchWithArray(Pair<String,Object>... params) {
		if (params!=null) {
			final int nparams = params.length;
			StringBuilder paramList = new StringBuilder();
			Object[] paramValues = new Object[nparams];
			for (int p=0; p<nparams; p++) {
				paramList.append(p==0 ? "" : ",").append(params[p].$1());
				paramValues[p] = params[p].$2();
			}
			qry.declareParameters(paramList.toString());
			return (RecordSet<R>) qry.executeWithArray(paramValues);
		} else {
			return (RecordSet<R>) qry.execute();
		}
	}

	@SuppressWarnings("unchecked")
	public RecordSet<R> fetchWithMap(LinkedHashMap<String,Object> params) {
		if (params!=null & params.size()>0) {
			StringBuilder paramList = new StringBuilder();
			int p = 0;
			for (Entry<String,Object> e : params.entrySet())
				paramList.append(++p==1 ? "" : ",").append(e.getKey());
			qry.declareParameters(paramList.toString());		
			return (RecordSet<R>) qry.executeWithMap(params);			
		} else {
			return (RecordSet<R>) qry.execute();
		}
	}
	
}
