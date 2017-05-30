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

import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;

import org.judal.metadata.JoinType;
import org.judal.metadata.NameAlias;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.StorageContext;

import com.knowgate.tuples.Pair;

/**
 * Interface for DataSource implementations that support tables
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface TableDataSource extends DataSource {

	/**
	 * Get table definition
	 * @param tableName String Table Name
	 * @return TableDef
	 * @throws JDOException
	 */
	TableDef getTableDef(String tableName) throws JDOException;

	/**
	 * Get schema meta data
	 * @return SchemaMetaData
	 * @throws JDOException
	 */
	SchemaMetaData getMetaData() throws JDOException;
	
	/**
	 * Set schema meta data
	 * @param smd SchemaMetaData
	 * @throws JDOException
	 */
	void setMetaData(SchemaMetaData smd) throws JDOException;
	
	/**
	 * Create new table definition
	 * @param tableName String
	 * @param options Map&lt;String,Object&gt;
	 * @return TableDef
	 * @throws JDOException
	 */
	TableDef createTableDef(String tableName, Map<String,Object> options) throws JDOException;

	/**
	 * Create a new Table using the given definition
	 * @param tableDef TableDef
	 * @param options Map&lt;String,Object&gt;
	 * @throws JDOException
	 */
	void createTable(TableDef tableDef, Map<String,Object> options) throws JDOException;

	/**
	 * Drop a Table
	 * @param tableName String Table Name
	 * @param cascade boolean Delete cascade dependent objects
	 * @throws JDOException
	 */
	void dropTable(String tableName, boolean cascade) throws JDOException;

	/**
	 * Truncate a table
	 * @param tableName String Table Name
	 * @param cascade boolean Delete cascade rows at other tables referencing any row of this table by mean of a foreign key
	 * @throws JDOException
	 */
	void truncateTable(String tableName, boolean cascade) throws JDOException;

	/**
	 * Open Table for Read/Write.
	 * Basic tables only support seeking records by primary key.
	 * @param recordInstance Record Instance of the Record subclass that will be used to read/write the Table
	 * @return Table
	 * @throws JDOException
	 */
	Table openTable(Record recordInstance) throws JDOException;

	/**
	 * Open Indexed Table for Read/Write
	 * Indexed tables support secondary indexes for seeking records.
	 * @param recordInstance Instance of the Record subclass that will be used to read/write the Table
	 * @return IndexableTable
	 * @throws JDOException
	 */
	IndexableTable openIndexedTable(Record recordInstance) throws JDOException;
	
	/**
	 * Open View for Read-Only.
	 * Basic views only support seeking records by primary key.
	 * @param recordInstance Record Instance of the Record subclass that will be used to read the View
	 * @return View
	 * @throws JDOException
	 */
	View openView(Record recordInstance) throws JDOException;

	/**
	 * Open Indexed View for Read-Only.
	 * Indexed views support secondary indexes for seeking records.
	 * @param recordInstance Instance of the Record subclass that will be used to read the View
	 * @return IndexableView
	 * @throws JDOException
	 */
	IndexableView openIndexedView(Record recordInstance) throws JDOException;

	/**
	 * Open Indexed View for Read-Only of two joined views.
	 * @param joinType JoinType enum INNER, LEFTOUTER, RIGHTOUTER, FULL
	 * @param result Record subclass that will be used to read the joined pair
	 * @param baseTable NameAlias Base table name and alias
	 * @param joinedTable NameAlias Joined table name and alias
	 * @param onColumns Pair&lt;String,String&gt; Variable number of columns to do the join
	 * @return IndexableView
	 * @throws JDOException
	 */
	IndexableView openJoinView(JoinType joinType, Record result, NameAlias baseTable, NameAlias joinedTable, Pair<String,String>... onColumns) throws JDOException;

}