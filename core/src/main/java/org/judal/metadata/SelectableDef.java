package org.judal.metadata;

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

import javax.jdo.metadata.TypeMetadata;

public interface SelectableDef extends TypeMetadata {

	/**
	 * @return ColumnDef[]
	 */
	@Override
	public ColumnDef[] getColumns();

	/**
	 * @return String Comma separated list of column names
	 */
	public String getColumnsStr();

	/**
	 * <p>Get column by name.</p>
	 * Name matching is case insensitive.
	 * @param columnName String
	 * @return ColumnDef
	 * @throws ArrayIndexOutOfBoundsException If no column with the given name is found.
	 */
	ColumnDef getColumnByName(String columnName) throws ArrayIndexOutOfBoundsException;
	
	/**
	 * <p>Get Column position at table or view.</p>
	 * @param columnName String
	 * @return int [1..columnCount()] or -1 if no column with such name exists
	 */
	int getColumnIndex(String columnName);

	PrimaryKeyDef getPrimaryKeyMetadata();

}