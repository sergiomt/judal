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

import javax.jdo.metadata.ColumnMetadata;

/**
 * <p>Interface for read only views with schema.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface View extends SchemalessView {

	ColumnMetadata[] columns();

	int columnsCount();

	ColumnMetadata getColumnByName (String columnName);

	/**
	 * <p>Get Column index given its name</p>
	 * @param columnName String
	 * @return Column Index[1..columnsCount()] or -1 if no column with such name was found.
	 */
	int getColumnIndex (String columnName);

}
