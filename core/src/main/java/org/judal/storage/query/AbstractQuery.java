package org.judal.storage.query;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.jdo.Query;
import javax.jdo.Extent;
import javax.jdo.JDOException;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.judal.storage.Param;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

import com.knowgate.typeutils.ObjectFactory;

public abstract class AbstractQuery implements Cloneable, Query {

	private static final long serialVersionUID = 1L;
	private static final Long ZERO = new Long(0L);
	
	private HashMap<String,Object> extensions;
	@SuppressWarnings("rawtypes")
	private Class resultClass;
	protected Constructor<? extends Record> recordConstructor;
	protected Object[] constructorParameters;
	private Class<Record> candidateClass;
	private String results;
	private String filter;
	private Predicate filterPredicate;
	private String grouping;
	private String ordering;
	private boolean ignoreCache;
	private Long fromIncl;
	private Long toExcl;
	private boolean unique;
	private boolean unmodifiable;
	@SuppressWarnings("rawtypes")
	private Iterable candidates;
	private HashMap<String,Object> parameters;
	private HashMap<Integer,String> paramIndexes;
	private ArrayList<AbstractQuery> subqueries;
	protected boolean endOfFetch;

	public AbstractQuery() {
		subqueries = new ArrayList<AbstractQuery>();
		candidates = null;
		candidateClass = null;
		resultClass = null;
		results = "*";
		filter = null;
		grouping = null;
		ordering = null;
		ignoreCache = false;
		fromIncl = ZERO;
		toExcl = null;
		unique = false;
		unmodifiable = false;
		parameters = null;
		paramIndexes = null;
		endOfFetch = true;
	}

	public abstract AbstractQuery clone();

	protected void clone(AbstractQuery source) {
		if (source.extensions==null) {
			this.extensions = null;
		} else {
			this.extensions = new HashMap<String,Object>();
			this.extensions.putAll(source.extensions);
		}
		this.resultClass = source.resultClass;
		this.recordConstructor = source.recordConstructor;
		if (source.constructorParameters==null) {
			this.constructorParameters = null;
		} else {
			this.constructorParameters = Arrays.copyOf(source.constructorParameters, source.constructorParameters.length);
		}
		this.candidateClass = source.candidateClass;
		this.results = source.results;
		this.filter = source.filter;
		this.filterPredicate = source.filterPredicate;
		this.grouping = source.grouping;
		this.ordering = source.ordering;
		this.ignoreCache = source.ignoreCache;
		this.fromIncl = source.fromIncl;
		this.toExcl = source.toExcl;
		this.unique = source.unique;
		this.unmodifiable = false;
		this.candidates = source.candidates;
		if (source.parameters==null) {
			this.parameters = null;
		} else {
			this.parameters = new HashMap<String,Object>();
			this.parameters.putAll(source.parameters);
		}
		if (source.paramIndexes==null) {
			this.paramIndexes = null;
		} else {
			this.paramIndexes = new HashMap<Integer,String>();
			this.paramIndexes.putAll(source.paramIndexes);
		}
		if (source.subqueries==null) {
			this.subqueries = null;
		} else {
			this.subqueries = new ArrayList<AbstractQuery>(source.subqueries.size());
			this.subqueries.addAll(source.subqueries);
		}
		this.endOfFetch = source.endOfFetch;
	}
	
	public abstract Record newRecord() throws NullPointerException, NoSuchMethodException, JDOException;

	public abstract Predicate newPredicate(Connective logicalConnective) throws JDOException;

	public Predicate newPredicate() throws JDOException {
		return newPredicate(Connective.NONE);
	}

	public Predicate newPredicate(Connective logicalConnective, String op, Param[] filterParameters) throws JDOException {
		if (logicalConnective==null)
			throw new NullPointerException("Logical connective cannot be null");
		if (logicalConnective.equals(Connective.NONE))
			throw new JDOUserException("Logical connective cannot be null");
		Predicate filtering = newPredicate(logicalConnective);
		for (Param p : filterParameters)
			try {
				filtering.add(p.getName(), op, p.getValue());
			} catch (UnsupportedOperationException | NoSuchMethodException | SecurityException | InstantiationException
					| IllegalAccessException | IllegalArgumentException | InvocationTargetException xcpt) {
				throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
			}
		return filtering;
	}
	
	public abstract Object source() throws JDOException;

	@Override
	public abstract RecordSet<? extends Record> execute() throws JDOException;
	
	@Override
	public void addExtension(String key, Object value) {
		if (null==extensions)
			extensions = new HashMap<String,Object>();
		extensions.put(key, value);
	}

	@Override
	public void addSubquery(Query sub, String arg1, String arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addSubquery(Query arg0, String arg1, String arg2, String arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addSubquery(Query arg0, String arg1, String arg2, String... arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addSubquery(Query arg0, String arg1, String arg2, Map arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cancel(Thread arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cancelAll() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void closeAll() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void compile() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareImports(String arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareParameters(String parameterNamesList) {
		String[] paramList = parameterNamesList.split(",");
		paramIndexes = new HashMap<Integer,String>(2*(paramList.length+1));
		parameters = new HashMap<String,Object>(2*(paramList.length+1));
		int p = 0;
		for (String param : paramList) {
			String paramName = param;
			if (param.indexOf(' ')>=0)
				paramName = param.split(" ")[1];
			paramIndexes.put(p++,  paramName);
			parameters.put(paramName, null);
		}
		filterPredicate = null;
	}

	@Override
	public void declareVariables(String arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long deletePersistentAll() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long deletePersistentAll(Object... arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long deletePersistentAll(Map arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean eof() {
		return endOfFetch;
	}

	@Override
	public RecordSet<? extends Record> execute(Object param1) {
		if (null==parameters)
			throw new JDOException("There are no declared parameters");
		else if (parameters.size()!=1)
			throw new JDOException("Query takes "+String.valueOf(parameters.size())+" but 1 provided");
		else
			parameters.put(paramIndexes.get(0), param1);
		return execute();
	}

	@Override
	public RecordSet<? extends Record> execute(Object param1, Object param2) {
		if (null==parameters)
			throw new JDOException("There are no declared parameters");
		else if (parameters.size()!=2)
			throw new JDOException("Query takes "+String.valueOf(parameters.size())+" but 2 provided");
		else {
			parameters.put(paramIndexes.get(0), param1);
			parameters.put(paramIndexes.get(1), param2);
		}
		return execute();
	}

	@Override
	public RecordSet<? extends Record> execute(Object param1, Object param2, Object param3) {
		if (null==parameters)
			throw new JDOException("There are no declared parameters");
		else if (parameters.size()!=2)
			throw new JDOException("Query takes "+String.valueOf(parameters.size())+" but 3 provided");
		else {
			parameters.put(paramIndexes.get(0), param1);
			parameters.put(paramIndexes.get(1), param2);
			parameters.put(paramIndexes.get(2), param3);
		}
		return execute();
	}

	@Override
	public RecordSet<? extends Record> executeWithArray(Object... params) {
		if (null==parameters)
			throw new JDOException("There are no declared parameters");
		else if (parameters.size()!=2)
			throw new JDOException("Query takes "+String.valueOf(parameters.size())+" but "+String.valueOf(params.length)+" provided");
		else
			for (int n=0; n<params.length; n++)
				parameters.put(paramIndexes.get(n), params[n]);
		return execute();
	}

	@Override
	public RecordSet<? extends Record> executeWithMap(Map params) {
		if (null==parameters)
			throw new JDOException("There are no declared parameters");
		else if (parameters.size()!=2)
			throw new JDOException("Query takes "+String.valueOf(parameters.size())+" but "+String.valueOf(params.size())+" provided");
		else {
			Iterator<String> iter = params.keySet().iterator();
			while (iter.hasNext()) {
				String key = iter.next();
				if (!parameters.containsKey(key))
					throw new JDOException("Unknown parameter "+key);
				parameters.put(key, params.get(key));
			}
		}
		return execute();
	}

	@Override
	public Integer getDatastoreReadTimeoutMillis() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getDatastoreWriteTimeoutMillis() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getIgnoreCache() {
		return ignoreCache;
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		// TODO Auto-generated method stub
		return null;
	}

	public Long getRangeFromIncl() {
		return fromIncl;
	}

	public Long getRangeToExcl() {
		return toExcl;
	}

	public int getMaxRows() {
		if (toExcl==null)
			return Integer.MAX_VALUE;
		else
			return (int) (toExcl.longValue()-fromIncl.longValue());
	}

	@Override
	public Boolean getSerializeRead() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isUnmodifiable() {
		return unmodifiable;
	}

	@Override
	public void setCandidates(Extent candidates) {
		this.candidates = candidates;
	}

	@Override
	public void setCandidates(Collection candidates) {
		this.candidates = candidates;		
	}

	public Object getCandidates() {
		return candidates;		
	}
	
	@Override
	public void setClass(Class candidateClass) {
		this.candidateClass = candidateClass;
	}

	@Override
	public void setDatastoreReadTimeoutMillis(Integer arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDatastoreWriteTimeoutMillis(Integer arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setExtensions(Map exts) {
		if (null==extensions)
			extensions = new HashMap<String,Object>();
		extensions.putAll(exts);
	}

	@Override
	public void setFilter(String filterExpression) {
		this.filter = filterExpression;
	}

	public void setFilter(Predicate filterPredicate) {
		this.filter = filterPredicate.getTextParametrized();
		this.filterPredicate = filterPredicate;
	}
	
	public String getFilter() {
		return filter;
	}
	
	@Override
	public void setGrouping(String groupingExpression) {
		this.grouping = groupingExpression;
	}

	public String getGrouping() {
		return grouping;
	}
	
	@Override
	public void setIgnoreCache(boolean ignore) {
		this.ignoreCache = ignore;
	}
	
	@Override
	public void setOrdering(String orderingExpression) {
		this.ordering = orderingExpression;
	}

	public String getOrdering() {
		return ordering;
	}

	public Object[] getParameters() {
		if (filterPredicate==null)
			if (parameters==null)
				return new Object[0];
			else
				return parameters.values().toArray(new Object[parameters.size()]);
		else
			return filterPredicate.getParameters();
	}
	
	@Override
	public void setRange(String fromIncToExc) {
		String[] fromto = fromIncToExc.split(",");
		setRange(Long.parseLong(fromto[0].trim()),Long.parseLong(fromto[1].trim()));
	}

	@Override
	public void setRange(long fromInc, long toExc) {
		this.fromIncl = new Long(fromInc);
		this.toExcl = new Long(toExc);
	}

	@Override
	public void setResult(String data) {
		this.results = data;
	}

	public void setResult(Iterable<String> members) {		
		this.results = String.join(",", members);
	}
	
	public String getResult() {
		return results;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void setResultClass(Class resultClass) throws ClassCastException {
		if (null==resultClass)
			throw new NullPointerException("Result Class cannot be null");
		if (Record.class.isAssignableFrom(resultClass)) {
			recordConstructor = (Constructor<? extends Record>) ObjectFactory.getConstructor((Class<? extends Object>) resultClass, new Class[0]);
			this.resultClass = resultClass;
		} else {
			throw new ClassCastException("Cannot cast from "+resultClass.getClass().getName()+" to "+Record.class.getName());
		}
	}

	@SuppressWarnings("unchecked")
	public void setResultClass(Class<? extends Record> resultClass, Class<?>... constructorParameterClasses) {
		if (null==resultClass)
			throw new NullPointerException("Result Class cannot be null");
		if (null==recordConstructor || null==constructorParameters || null==constructorParameterClasses) {
			if (Record.class.isAssignableFrom(resultClass)) {
				recordConstructor = (Constructor<? extends Record>) ObjectFactory.getConstructor((Class<? extends Object>) resultClass, new Class[0]);
				this.resultClass = resultClass;
			} else {
				throw new ClassCastException("Cannot cast from "+resultClass.getClass().getName()+" to "+Record.class.getName());
			}
		} else {
			this.resultClass = resultClass;
			recordConstructor = (Constructor<? extends Record>) ObjectFactory.getConstructor((Class<? extends Object>) resultClass, constructorParameterClasses);
		}
	}
	
	public Class getResultClass() {
		return resultClass;
	}

	@Override
	public void setSerializeRead(Boolean arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setUnique(boolean unique) {
		this.unique = unique;
	}

	@Override
	public void setUnmodifiable() {
		unmodifiable = true;
	}
	
}
