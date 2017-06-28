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

import javax.jdo.JDOException;

import org.judal.metadata.IndexDef.Using;
import org.judal.storage.Param;

/**
 * <p>Interface for indexable tables.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface IndexableTable extends Table, IndexableView {

	/**
	 * <p>Update records.</p>
	 * @param values Param[] Values to be updated
	 * @param where Param[] Values for the filter clause
	 * @return int Count of updated records
	 * @throws JDOException
	 */
	int update(Param[] values, Param[] where) throws JDOException;

	/**
	 * <p>Delete records.</p>
	 * @param where Param[] Values for the filter clause
	 * @return int Count of deleted records
	 * @throws JDOException
	 */
	int delete(Param[] where) throws JDOException;
	
	/**
	 * <p>Create index on a column.</p>
	 * @param indexName String
	 * @param unique boolean
	 * @param indexUsing Using
	 * @param columns String&hellip; Names of columns forming the index
	 * @throws JDOException
	 */
	void createIndex(String indexName, boolean unique, Using indexUsing, String... columns) throws JDOException;

	/**
	 * <p>Drop existing index.</p>
	 * @param indexName String
	 * @throws JDOException
	 */
	void dropIndex(String indexName) throws JDOException;
	
}
