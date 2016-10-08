package org.judal.storage;

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

import java.util.Map;
import java.util.Map.Entry;

import javax.jdo.JDOException;

import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;

/**
 * 
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface TableDataSource extends DataSource {

	TableDef getTableDef(String tableName) throws JDOException;

	SchemaMetaData getMetaData() throws JDOException;
	
	void setMetaData(SchemaMetaData oSmd) throws JDOException;
	
	TableDef createTableDef(String tableName, Map<String,Object> options) throws JDOException;

	void createTable(TableDef tableDef, Map<String,Object> options) throws JDOException;

	void dropTable(String tableName, boolean cascade) throws JDOException;

	void truncateTable(String tableName, boolean cascade) throws JDOException;

	Table openTable(Record recordInstance) throws JDOException;

	IndexableTable openIndexedTable(Record recordInstance) throws JDOException;
	
	View openView(Record recordInstance) throws JDOException;

	IndexableView openIndexedView(Record recordInstance) throws JDOException;

	IndexableView openInnerJoinView(Record recordInstance1, String joinedTableName, Entry<String,String> column) throws JDOException;

	IndexableView openOuterJoinView(Record recordInstance1, String joinedTableName, Entry<String,String> column) throws JDOException;

	IndexableView openInnerJoinView(Record recordInstance1, String joinedTableName, Entry<String,String>[] columns) throws JDOException;

	IndexableView openOuterJoinView(Record recordInstance1, String joinedTableName, Entry<String,String>[] columns) throws JDOException;
	
}
