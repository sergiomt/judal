package org.judal.storage.table;

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

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;
import org.judal.storage.Param;
import org.judal.storage.keyvalue.ReadOnlyBucket;

public interface View extends ReadOnlyBucket {
	  
 	  ColumnDef[] columns();
 
	  int columnsCount();

	  ColumnDef getColumnByName (String columnName);

	  /**
		* <p>Get Column index given its name</p>
		* @param columnName String
		* @return Column Index[1..columnsCount()] or -1 if no column with such name was found.
	   */
	  int getColumnIndex (String columnName);
	  
 	  boolean exists(Param... keys) throws JDOException;

 	  long count(String indexColumnName, Object valueSearched) throws JDOException;

 	  <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched) throws JDOException;

 	  <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched, int maxrows, int offset) throws JDOException;
 	  
 	  <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueFrom, Object valueTo) throws JDOException, IllegalArgumentException;

 	  <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueFrom, Object valueTo, int maxrows, int offset) throws JDOException, IllegalArgumentException;

 	  Class<? extends Record> getResultClass();

}
