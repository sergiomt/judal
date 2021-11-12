package org.judal.storage.relational;

/*
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

import javax.jdo.JDOException;

import javax.jdo.Query;
import org.judal.storage.query.Predicate;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

/**
 * <p>Interface for relational views.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface RelationalView extends IndexableView {

	  /**
	   * <p>Create new Query.</p>
	   * @return AbstractQuery
	   * @throws JDOException
	   */
	  Query newQuery() throws JDOException;
	  
	  /**
	   * <p>Create new Predicate.</p>
	   * @return Predicate
	   */
	  Predicate newPredicate();
	  
	  /**
	   * <p>Fetch records which match a given query.</p>
	   * @param query AbstractQuery
	   * @return RecordSet&lt;&lt;? extends Record&gt;&gt;
	   * @throws JDOException
	   */
	  <R extends Record> RecordSet<R> fetch(Query query) throws JDOException;
	
	  /**
	   * <p>Fetch first record which matches a given query.</p>
	   * @param query AbstractQuery
	   * @return R extends Record or null if no record was returned by the query
	   * @throws JDOException
	   */
	  <R extends Record> R fetchFirst(Query query) throws JDOException;

	  /**
	   * <p>Count records matching a Predicate.</p>
	   * @param filterPredicate Predicate
	   * @return Long
	   * @throws JDOException
	   */
	  Long count (Predicate filterPredicate) throws JDOException;
	  
	  /**
	   * <p>Get average of values in a column for records matching a Predicate.</p>
	   * @param result String Column name
	   * @param filterPredicate Predicate
	   * @return Number
	   * @throws JDOException
	   */
	  Number avg (String result, Predicate filterPredicate) throws JDOException;

	  /**
	   * <p>Get sum of values in a column for records matching a Predicate.</p>
	   * @param result String Column name
	   * @param filterPredicate Predicate
	   * @return Number
	   * @throws JDOException
	   */
	  Number sum (String result, Predicate filterPredicate) throws JDOException;
	  
	  /**
	   * <p>Get maximum value in a column for records matching a Predicate.</p>
	   * @param result String Column name
	   * @param filterPredicate Predicate
	   * @return Object
	   * @throws JDOException
	   */
	  Object max (String result, Predicate filterPredicate) throws JDOException;

	  /**
	   * <p>Get minimum value in a column for records matching a Predicate.</p>
	   * @param result String Column name
	   * @param filterPredicate Predicate
	   * @return Object
	   * @throws JDOException
	   */
	  Object min (String result, Predicate filterPredicate) throws JDOException;

}
