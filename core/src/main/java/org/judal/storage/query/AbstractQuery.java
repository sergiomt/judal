package org.judal.storage.query;

/**
 * © Copyright 2016 the original author.
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
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.judal.storage.Param;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

import com.knowgate.typeutils.ObjectFactory;

/**
 * <p>Partial implementation of JDO Query interface.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
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
	private boolean serializeReads;
	private Integer readTimeout;
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
		filterPredicate = null;
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
		this.serializeReads = source.serializeReads;
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
	
	/**
	 * <p>Create a new instance of Record subclass used to iterate through this query results.</p>
	 * @return Record
	 * @throws NullPointerException
	 * @throws NoSuchMethodException
	 * @throws JDOException
	 */
	public abstract Record newRecord() throws NullPointerException, NoSuchMethodException, JDOException;

	/**
	 * <p>Create new query predicate.</p>
	 * @param logicalConnective Connective
	 * @return Predicate
	 * @throws JDOException
	 */
	public abstract Predicate newPredicate(Connective logicalConnective) throws JDOException;

	/**
	 * <p>Create new query predicate with Connective.NONE.</p>
	 * @return Predicate
	 * @throws JDOException
	 */
	public Predicate newPredicate() throws JDOException {
		return newPredicate(Connective.NONE);
	}

	/**
	 * <p>Create a new query predicate and add to it the given parameters joined by a logical connective.</p>
	 * 
	 * @param logicalConnective Connective
	 * @param op String One of Operator class static members
	 * @param filterParameters Param[]
	 * @return Predicate
	 * @throws JDOException
	 */
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
	
	/**
	 * <p>Get query source code as required by the implementation.</p>
	 * @return Object For example a SQL statement in case of JDBC.
	 * @throws JDOException
	 */
	public abstract Object source() throws JDOException;

	@Override
	public void compile() {
		if (filterPredicate!=null)
			filter = filterPredicate.toString();
	}

	/**
	 * @param parameterNamesList String Comma separated list of parameter names
	 */
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

	/**
	 * <p>get whether read reached end of results.</p>
	 * This method is intended to be used for pagination purposes using range with queries.
	 * Every page except the last must return eof = false.
	 * This way the client application know when it has reached the last page.
	 * @return boolean <b>true</b> if the query returned as many results as present in the database matching the search criteria
	 * or <b>false</b> if more results were present but excluded from the results because of explicit limitation of maximum records returned.
	 */
	public boolean eof() {
		return endOfFetch;
	}

	/**
	 * <p>Execute this query and return a RecordSet with the results.</p>
	 * @return RecordSet&lt;? extends Record&gt;
	 * @throws JDOException
	 */
	@Override
	public abstract RecordSet<? extends Record> execute() throws JDOException;
	
	@Override
	public void addExtension(String key, Object value) {
		if (null==extensions)
			extensions = new HashMap<String,Object>();
		extensions.put(key, value);
	}

	/**
	 * <p>Bind one parameter and execute this query.</p>
	 * declareParameters() must have been called before execute()
	 * @param param1 Object
	 * @return RecordSet&lt;? extends Record&gt;
	 * @throws JDOException
	 */
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

	/**
	 * <p>Bind two parameters and execute this query.</p>
	 * declareParameters() must have been called before execute()
	 * @param param1 Object
	 * @param param2 Object
	 * @return RecordSet&lt;? extends Record&gt;
	 * @throws JDOException
	 */
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

	/**
	 * <p>Bind three parameters and execute this query.</p>
	 * declareParameters() must have been called before execute()
	 * @param param1 Object
	 * @param param2 Object
	 * @param param3 Object
	 * @return RecordSet&lt;? extends Record&gt;
	 * @throws JDOException
	 */
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

	/**
	 * <p>Bind a variable number of parameters and execute this query.</p>
	 * declareParameters() must have been called before executeWithArray()
	 * @param params Object&hellip;
	 * @return RecordSet&lt;? extends Record&gt;
	 * @throws JDOException
	 */
	@Override
	public RecordSet<? extends Record> executeWithArray(Object... params) {
		if (null==parameters)
			throw new JDOException("There are no declared parameters");
		else
			for (int n=0; n<params.length; n++)
				parameters.put(paramIndexes.get(n), params[n]);
		return execute();
	}

	/**
	 * <p>Bind a variable number of parameters and execute this query.</p>
	 * declareParameters() must have been called before executeWithMap()
	 * @param params Map&lt;String,Object&gt;
	 * @return RecordSet&lt;? extends Record&gt;
	 * @throws JDOException
	 */
	@Override
	public RecordSet<? extends Record> executeWithMap(Map params) {
		if (null==parameters)
			throw new JDOException("There are no declared parameters");
		else if (parameters.size()!=2)
			throw new JDOException("Query takes "+String.valueOf(parameters.size())+" but "+String.valueOf(params.size())+" provided");
		else {
			@SuppressWarnings("unchecked")
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

	/**
	 * @return boolean
	 */
	@Override
	public boolean getIgnoreCache() {
		return ignoreCache;
	}

	/**
	 * <p>Index of first row that will be fetched (inclusive).</p>
	 * @return Long [0..n-1]
	 */
	public Long getRangeFromIncl() {
		return fromIncl;
	}

	/**
	 * <p>Index of last row that will be fetched (exclusive).</p>
	 * @return Long  [1..n]
	 */
	public Long getRangeToExcl() {
		return toExcl;
	}

	/**
	 * <p>Maximum number of rows to be fetched.</p>
	 * If there is a rangeToExcl than rangeToExcl minus rangeFromIncl will be returned.
	 * Else this function will return Integer.MAX_VALUE
	 * @return int [0..Integer.MAX_VALUE]
	 */
	public int getMaxRows() {
		if (toExcl==null)
			return Integer.MAX_VALUE;
		else
			return (int) (toExcl.longValue()-fromIncl.longValue());
	}

	/**
	 * @return Boolean
	 */
	@Override
	public Boolean getSerializeRead() {
		return serializeReads;
	}

	/**
	 * @param serialize Boolean
	 */
	@Override
	public void setSerializeRead(Boolean serialize) {
		serializeReads = serialize;
	}

	/**
	 * <p>Get whether this query allows any modification in its definition.</p>
	 * If this query is unmodifiable then an attempt to call any method that
	 * modifies the internal state should raise a JDOUserException
	 * @return boolean
	 */
	@Override
	public boolean isUnmodifiable() {
		return unmodifiable;
	}

	/**
	 * @param candidates Extent
	 */
	@Override
	public void setCandidates(@SuppressWarnings("rawtypes") Extent candidates) {
		this.candidates = candidates;
	}

	/**
	 * @param candidates Collection
	 */
	@Override
	public void setCandidates(@SuppressWarnings("rawtypes") Collection candidates) {
		this.candidates = candidates;		
	}

	/**
	 * @return candidates Object
	 */
	public Object getCandidates() {
		return candidates;		
	}
	
	/**
	 * <p>Set class of candidate instances.</p>
	 * @param candidateClass Class
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void setClass(@SuppressWarnings("rawtypes") Class candidateClass) {
		this.candidateClass = candidateClass;
	}

	/**
	 * @param exts Map&lt;String,Object&gt;
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void setExtensions(@SuppressWarnings("rawtypes") Map exts) {
		if (null==extensions)
			extensions = new HashMap<String,Object>();
		extensions.putAll(exts);
	}

	/**
	 * <p>Set filter expression using native query language.</p>
	 * For JDBC implementations filterExpression must be the expression
	 * in the WHERE clause of the SELECT statement.
	 * @param filterExpression String
	 */
	@Override
	public void setFilter(String filterExpression) {
		this.filter = filterExpression;
	}

	/**
	 * <p>Set filter expression using a Predicate instance.</p>
	 * The implementation will translate the Predicate into a
	 * query which is executable by the storage technology in use.
	 * @param filterPredicate Predicate
	 */
	public void setFilter(Predicate filterPredicate) {
		this.filter = filterPredicate.getTextParametrized();
		this.filterPredicate = filterPredicate;
	}
	
	/**
	 * <p>Get native filter expression as constructed by the implementation.</p>
	 * @return String
	 */
	public String getFilter() {
		return filter;
	}

	/**
	 * <p>Get filter Predicate.</p>
	 * @return Predicate
	 */
	public Predicate getFilterPredicate() {
		return filterPredicate;
	}
	
	/**
	 * <p>Set grouping expression using native query language.</p>
	 * For JDBC implementations filterExpression must be the expression
	 * in the GROUP BY clause of the SELECT statement.
	 * @param groupingExpression String
	 */
	@Override
	public void setGrouping(String groupingExpression) {
		this.grouping = groupingExpression;
	}

	/**
	 * @return String
	 */
	public String getGrouping() {
		return grouping;
	}
	
	/**
	 * @param ignore boolean
	 */
	@Override
	public void setIgnoreCache(boolean ignore) {
		this.ignoreCache = ignore;
	}
	
	/**
	 * <p>Set grouping expression using native query language.</p>
	 * For JDBC implementations filterExpression must be the expression
	 * in the ORDER BY clause of the SELECT statement.
	 * @param orderingExpression String
	 */
	@Override
	public void setOrdering(String orderingExpression) {
		this.ordering = orderingExpression;
	}

	/**
	 * @return String
	 */
	public String getOrdering() {
		return ordering;
	}

	/**
	 * <p>Get array with values assigned to parameters.</p>
	 * @return Object[]
	 */
	public Object[] getParameters() {
		if (filterPredicate==null)
			if (parameters==null)
				return new Object[0];
			else
				return parameters.values().toArray(new Object[parameters.size()]);
		else
			return filterPredicate.getParameters();
	}
	
	/**
	 * <p>Set query range.</p>
	 * @param fromIncToExc String Two positive integers separated by a comma
	 * @throws ArrayIndexOutOfBoundsException If lower bound is less than zero or upper bound is strictly less than lower bound.
	 * @throws NumberFormatException
	 */
	@Override
	public void setRange(String fromIncToExc) throws NumberFormatException, ArrayIndexOutOfBoundsException {
		String[] fromto = fromIncToExc.split(",");
		final long fromRow = Long.parseLong(fromto[0].trim());
		final long toRow = Long.parseLong(fromto[1].trim());
		if (fromRow<0)
			throw new ArrayIndexOutOfBoundsException("AbstractQuery.setRange() Lower bound must be greater than or equal to zero");
		if (toRow<fromRow)
			throw new ArrayIndexOutOfBoundsException("AbstractQuery.setRange() Upper bound must be greater than or equal to lower bound");
		setRange(fromRow,toRow);
	}

	/**
	 * <p>Set query range.</p>
	 * @param fromInc long Lower bound (inclusive)
	 * @param toExc long Upper bound (exclusive)
	 * @throws ArrayIndexOutOfBoundsException If lower bound is less than zero or upper bound is strictly less than lower bound.
	 * @throws NumberFormatException
	 */
	@Override
	public void setRange(long fromInc, long toExc) {
		this.fromIncl = new Long(fromInc);
		this.toExcl = new Long(toExc);
	}

	/**
	 * <p>Set query timeout in milliseconds.</p>
	 * @return Integer
	 */
	@Override
	public Integer getDatastoreReadTimeoutMillis() {
		return readTimeout;
	}

	/**
	 * <p>Set query timeout in milliseconds.</p>
	 * @param timeout Integer
	 * @throws JDOUnsupportedOptionException If query timeout is not supported by the implementation
	 */
	@Override
	public void setDatastoreReadTimeoutMillis(Integer timeout) throws JDOUnsupportedOptionException {
		readTimeout = timeout;
	}

	/**
	 * <p>Set results using a native expression from the implementation engine.</p>
	 * For JDBC implementation this may be a comma separated column list, an asterisk
	 * or any other string valid right after SELECT command in a SQL statement.
	 * @param data String
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public void setResult(String data) throws JDOUnsupportedOptionException {
		this.results = data;
	}

	/**
	 * <p>Set results using an iterable list of column names.</p>
	 * @param members Iterable&lt;String&gt;
	 * @throws JDOUnsupportedOptionException
	 */
	public void setResult(Iterable<String> members) {		
		this.results = String.join(",", members);
	}
	
	/**
	 * @return String
	 */
	public String getResult() {
		return results;
	}
	
	/**
	 * <p>Set results using a subclass of Record.</p>
	 * The provided class must have a default parameterless constructor.
	 * The columns fetched by this query will be defined by the fetchGroup() method of resultClass
	 * @param resultClass Class&lt;? extends Record&gt;
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void setResultClass(@SuppressWarnings("rawtypes") Class resultClass) throws ClassCastException {
		if (null==resultClass)
			throw new NullPointerException("Result Class cannot be null");
		if (Record.class.isAssignableFrom(resultClass)) {
			recordConstructor = (Constructor<? extends Record>) ObjectFactory.getConstructor((Class<? extends Object>) resultClass, new Class[0]);
			this.resultClass = resultClass;
		} else {
			throw new ClassCastException("Cannot cast from "+resultClass.getClass().getName()+" to "+Record.class.getName());
		}
	}

	/**
	 * <p>Set results using a subclass of Record.</p>
	 * The columns fetched by this query will be defined by the fetchGroup() method of resultClass
	 * @param resultClass Class&lt;? extends Record&gt;
	 * @param constructorParameterClasses Class&lt;?&gt;&hellip; List of classes of the parameters taken by the constructor which must be used by this query when adding new results during fetch.
	 */
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
	
	/**
	 * @return Class&lt;? extends Record&gt;
	 */
	@SuppressWarnings("rawtypes")
	public Class getResultClass() {
		return resultClass;
	}

	@Override
	public void setUnique(boolean unique) {
		this.unique = unique;
	}

	@Override
	public void setUnmodifiable() {
		unmodifiable = true;
	}
	

	@Override
	public void addSubquery(Query sub, String arg1, String arg2) {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.addSubquery()");
	}

	@Override
	public void addSubquery(Query arg0, String arg1, String arg2, String arg3) {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.addSubquery()");		
	}

	@Override
	public void addSubquery(Query arg0, String arg1, String arg2, String... arg3) {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.addSubquery()");				
	}

	@Override
	public void addSubquery(Query arg0, String arg1, String arg2, Map arg3) {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.addSubquery()");				
	}

	@Override
	public void cancel(Thread arg0) {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.cancel()");		
	}

	@Override
	public void cancelAll() {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.cancelAll()");		
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
	public void declareImports(String arg0) {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.declareImports()");
	}

	@Override
	public void declareVariables(String arg0) {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.declareVariables()");		
	}

	@Override
	public long deletePersistentAll() {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.deletePersistentAll()");		
	}

	@Override
	public long deletePersistentAll(Object... arg0) {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.deletePersistentAll()");		
	}

	@Override
	public long deletePersistentAll(Map arg0) {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.deletePersistentAll()");		
	}

	@Override
	public void setDatastoreWriteTimeoutMillis(Integer arg0) {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.setDatastoreWriteTimeoutMillis()");				
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.getPersistenceManager()");				
	}

	@Override
	public Integer getDatastoreWriteTimeoutMillis() {
		// TODO Auto-generated method stub
		throw new JDOUnsupportedOptionException("AbstractQuery.getDatastoreWriteTimeoutMillis()");				
	}
	
}
