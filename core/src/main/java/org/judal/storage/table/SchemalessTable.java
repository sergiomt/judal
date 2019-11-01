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
import javax.jdo.metadata.PrimaryKeyMetadata;

import org.judal.storage.Param;
import org.judal.storage.keyvalue.Bucket;

/**
 * <p>Interface for tables without schema.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface SchemalessTable extends Bucket, SchemalessView {

	/**
	 * @return PrimaryKeyMetadata
	 */
	PrimaryKeyMetadata getPrimaryKey();

	/**
	 * <p>Insert a new Record.</p>
	 * @param params Param&hellip; Values to be inserted
	 * @throws JDOException If another Record with the same primary key already exists
	 */
	void insert(Param... params) throws JDOException;
}
