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

import org.judal.storage.Param;

import javax.jdo.FetchGroup;

/**
 * <p>Interface for indexable views.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface SchemalessIndexableView extends SchemalessView {

	/**
	 * <p>Fetch records which columns contain a given set of values.</p>
	 * Using each Param name as a column name, return those records for which
	 * each column has the value held in its corresponding Param.
	 * This is to say, for example, if the Engine is JDBC and params are
	 * Param("col1", 1, "value1"), Param("col2", 2, "value2"), Param("col3", 3, "value3")
	 * then the resulting WHERE clause for the underlying SQL statement will be
	 * col1='value1' AND col2='value2' AND col3='value3' 
	 * @param fetchGroup FetchGroup Columns to be fetched
	 * @param maxrows int Maximum number of records to return
	 * @param offset int
	 * @param params Param&hellip;
	 * @return RecordSet&lt;&lt;? extends Record&gt;&gt;
	 * @throws JDOException
	 */
	<R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, int maxrows, int offset, Param... params) throws JDOException;

}