package org.judal.storage.table;

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

import java.util.Map;

import javax.jdo.JDOException;

import org.judal.metadata.IndexDef;
import org.judal.metadata.JoinType;
import org.judal.metadata.NameAlias;
import org.judal.storage.DataSource;
import org.judal.storage.FieldHelper;

import org.judal.storage.Pair;

/**
 * Interface for DataSource implementations that support tables without schema
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface SchemalessTableDataSource extends DataSource {

	/**
	 * Create new index definition
	 * @param indexName String
	 * @param tableName String
	 * @param columns Iterable&ldquo;String&rdquo;
	 * @param indexType IndexDef.Type
	 * @param using IndexDef.Using
	 * @return IndexDef
	 * @throws JDOException
	 */
	IndexDef createIndexDef(String indexName, String tableName, Iterable<String> columns, IndexDef.Type indexType, IndexDef.Using using) throws JDOException;

	/**
	 * Create a new SchemalessTable
	 * @param tableName Strinmg
	 * @param options Map&lt;String,Object&gt;
	 * @throws JDOException
	 */
	void createTable(String tableName, Map<String,Object> options) throws JDOException;

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
	 * @return SchemalessTable
	 * @throws JDOException
	 */
	SchemalessTable openTable(Record recordInstance) throws JDOException;

	/**
	 * Open Indexed Table for Read/Write
	 * Indexed tables support secondary indexes for seeking records.
	 * @param recordInstance Instance of the Record subclass that will be used to read/write the Table
	 * @return SchemalessIndexableTable
	 * @throws JDOException
	 */
	SchemalessIndexableTable openIndexedTable(Record recordInstance) throws JDOException;

	/**
	 * Open View for Read-Only.
	 * Basic views only support seeking records by primary key.
	 * @param recordInstance Record Instance of the Record subclass that will be used to read the View
	 * @return SchemalessView
	 * @throws JDOException
	 */
	SchemalessView openView(Record recordInstance) throws JDOException;

	/**
	 * Open Indexed View for Read-Only.
	 * Indexed views support secondary indexes for seeking records.
	 * @param recordInstance Instance of the Record subclass that will be used to read the View
	 * @return SchemalessIndexableView
	 * @throws JDOException
	 */
	SchemalessIndexableView openIndexedView(Record recordInstance) throws JDOException;

	/**
	 * Open Indexed View for Read-Only of two joined views.
	 * @param joinType JoinType enum INNER, LEFTOUTER, RIGHTOUTER, FULL
	 * @param result Record subclass that will be used to read the joined pair
	 * @param baseTable NameAlias Base table name and alias
	 * @param joinedTable NameAlias Joined table name and alias
	 * @param onColumns Pair&lt;String,String&gt; Variable number of columns to do the join
	 * @return SchemalessIndexableView 
	 * @throws JDOException
	 */
	SchemalessIndexableView  openJoinView(JoinType joinType, Record result, NameAlias baseTable, NameAlias joinedTable, @SuppressWarnings("unchecked") Pair<String,String>... onColumns) throws JDOException;

	/**
	 * Get instance of field helper (if any) for the data source
	 * @return FieldHelper or <b>null</b> if there is no FieldHelper defined for this data source
	 * @throws JDOException
	 */
	FieldHelper getFieldHelper() throws JDOException;

}
