package org.judal.storage.relational;

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

import java.util.Map.Entry;

import javax.jdo.JDOException;

import org.judal.metadata.IndexDef;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;

/**
 * Basic interface for relational DataSource implementations
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface RelationalDataSource extends TableDataSource {

	IndexDef createIndexDef(String indexName, String tableName, String[] columns, boolean unique) throws JDOException;

	/**
	 * Create new unique or non-unique index
	 * @param indexDef IndexDef
	 * @throws JDOException
	 */
	void createIndex(IndexDef indexDef) throws JDOException;

	/**
	 * Drop index
	 * @param indexName String
	 * @param tableName String
	 * @throws JDOException
	 */
	void dropIndex(String indexName, String tableName) throws JDOException;

	/**
	 * Open relational table for read/write.
	 * @param recordInstance Record
	 * @return RelationalTable
	 * @throws JDOException
	 */
	RelationalTable openRelationalTable(Record recordInstance) throws JDOException;
	
	/**
	 * Open relational view for read-only.
	 * @param recordInstance Record
	 * @return RelationalView
	 * @throws JDOException
	 */
	RelationalView openRelationalView(Record recordInstance) throws JDOException;
	
	/**
	 * Open relational view for read-only of two inner joined views.
	 * @param recordInstance1 Record subclass that will be used to read the joined pair
	 * @param joinedTableName String Joined table name
	 * @param column Entry&lt;String,String&gt; Column name at base table and column name at the joined table
	 * @return RelationalView
	 * @throws JDOException
	 */
	RelationalView openInnerJoinView(Record recordInstance1, String joinedTableName, Entry<String,String> column) throws JDOException;

	/**
	 * Open relational view for read-only of two outer joined views.
	 * @param recordInstance1 Record subclass that will be used to read the joined pair
	 * @param joinedTableName String Joined table name
	 * @param column Entry&lt;String,String&gt; Column name at base table and column name at the joined table
	 * @return RelationalView
	 * @throws JDOException
	 */
	RelationalView openOuterJoinView(Record recordInstance1, String joinedTableName, Entry<String,String> column) throws JDOException;

	RelationalView openInnerJoinView(Record recordInstance1, String joinedTableName, Entry<String,String>[] columns) throws JDOException;

	RelationalView openOuterJoinView(Record recordInstance1, String joinedTableName, Entry<String,String>[] columns) throws JDOException;

	/**
	 * Get Relational Database Management System Id (given by the implementation)
	 * @return int
	 */
	int getRdbmsId();

}