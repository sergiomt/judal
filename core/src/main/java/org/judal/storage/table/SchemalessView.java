package org.judal.storage.table;

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


import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.judal.storage.Param;
import org.judal.storage.keyvalue.ReadOnlyBucket;

/**
 * <p>Interface for read only views without schema.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface SchemalessView extends ReadOnlyBucket {

	final String NUM_ROWS = "NUM_ROWS";

	/**
	 * Check if a record matching all the given column name and value pairs exist
	 * @param keys Param&hellip;
	 * @return boolean
	 * @throws JDOException
	 */
	boolean exists(Param... keys) throws JDOException;

	/**
	 * Count number of records with a given value at the specified column
	 * @param indexColumnName String
	 * @param valueSearched Object
	 * @return long
	 * @throws JDOException
	 */
	long count(String indexColumnName, Object valueSearched) throws JDOException;

	/**
	 * Fetch a static snapshot or records with a given value at the specified column
	 * @param fetchGroup FetchGroup Determines the columns that will be fetched, used for restricting the amount of data transfered from the server
	 * @param indexColumnName String
	 * @param valueSearched Object
	 * @return RecordSet&lt;R extends Record&gt;
	 * @throws JDOException
	 */
	<R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched) throws JDOException;

	/**
	 * Fetch a static snapshot or records with a given value at the specified column starting from an offset and up to a maximum number of records
	 * @param fetchGroup FetchGroup Determines the columns that will be fetched, used for restricting the amount of data transfered from the server
	 * @param indexColumnName String
	 * @param valueSearched Object
	 * @param maxrows Positive integer
	 * @param offset Positive integer
	 * @return RecordSet&lt;R extends Record&gt;
	 * @throws JDOException
	 */	
	<R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched, int maxrows, int offset) throws JDOException;

	/**
	 * Fetch a static snapshot or records with value in a given range at the specified column
	 * @param fetchGroup FetchGroup Determines the columns that will be fetched, used for restricting the amount of data transfered from the server
	 * @param indexColumnName String
	 * @param valueFrom Comparable
	 * @param valueTo Comparable
	 * @return RecordSet&lt;R extends Record&gt;
	 * @throws JDOException
	 */	
	<R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Comparable<?> valueFrom, Comparable<?> valueTo) throws JDOException, IllegalArgumentException;

	/**
	 * Fetch a static snapshot or records with value in a given range at the specified column starting from an offset and up to a maximum number of records
	 * @param fetchGroup FetchGroup Determines the columns that will be fetched, used for restricting the amount of data transfered from the server
	 * @param indexColumnName String
	 * @param valueFrom Comparable
	 * @param valueTo Comparable
	 * @param maxrows Positive integer
	 * @param offset Positive integer
	 * @return RecordSet&lt;R extends Record&gt;
	 * @throws JDOException
	 */	
	<R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Comparable<?> valueFrom, Comparable<?> valueTo, int maxrows, int offset) throws JDOException, IllegalArgumentException;

	/**
	 * Class used by the fetch methods to return results
	 * @return Class&lt;R extends Record&gt;
	 */
	Class<? extends Record> getResultClass();

}
