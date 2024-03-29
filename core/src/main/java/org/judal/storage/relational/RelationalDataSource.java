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

import org.judal.metadata.IndexDef;
import org.judal.metadata.JoinType;
import org.judal.metadata.NameAlias;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;

import org.judal.storage.Pair;

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
	 * Open relational view for read-only.
	 * @param recordInstance Record
	 * @param alias String Alias to be given to the view when used in a query
	 * @return RelationalView
	 * @throws JDOException
	 */
	RelationalView openRelationalView(Record recordInstance, String alias) throws JDOException;

	/**
	 * Open Relational View for read-only of two joined views.
	 * @param joinType JoinType enum INNER, OUTER
	 * @param result Record subclass that will be used to read the joined pair
	 * @param baseTable NameAlias Base table name and alias
	 * @param joinedTable NameAlias Joined table name and alias
	 * @param onColumns Pair&lt;String,String&gt; Variable number of columns to do the join
	 * @return RelationalView 
	 * @throws JDOException
	 */
	RelationalView  openJoinView(JoinType joinType, Record result, NameAlias baseTable, NameAlias joinedTable, Pair<String,String>... onColumns) throws JDOException;
	
	/**
	 * Get Relational Database Management System Id (given by the implementation)
	 * @return int
	 */
	int getRdbmsId();
	
}