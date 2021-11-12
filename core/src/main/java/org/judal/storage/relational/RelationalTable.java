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
import javax.jdo.Query;

import org.judal.storage.Param;
import org.judal.storage.table.IndexableTable;

/**
 * <p>Interface for relational tables.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface RelationalTable extends IndexableTable,RelationalView {

	/**
	 * <p>Update records filtered by a Query.</p>
	 * @param values Param[] Values tu be updated
	 * @param filter AbstractQuery
	 * @return int Count of updated records
	 * @throws JDOException
	 */
	int update(Param[] values, Query filter) throws JDOException;

	/**
	 * <p>Delete records filtered by a Query.</p>
	 * @param filter AbstractQuery
	 * @return int Count of deleted records
	 * @throws JDOException
	 */
	int delete(Query filter) throws JDOException;

}
