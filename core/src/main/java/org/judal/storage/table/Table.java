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


import org.judal.storage.keyvalue.Bucket;

/**
 * <p>Interface for read/write tables with schema.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface Table extends Bucket, View, SchemalessTable {

	/**
	 * @return String Name of column holding the Timestamp of when the record was created
	 */
	String getTimestampColumnName ();

	/**
	 * <p>This column, if set, won't be read by load operation nor modified by store operations.</p>
	 * The content of this column must be the timestamp when the Record was created.
	 * This value must not be modified at anytime afterwards.
	 * But can be read using fetch operations.
	 * @param columnName String Column name
	 * @throws IllegalArgumentException
	 */
	void setTimestampColumnName(String columnName) throws IllegalArgumentException;

}
