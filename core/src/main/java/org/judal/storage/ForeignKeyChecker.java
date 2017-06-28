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

import javax.jdo.JDOException;

/**
 * <p>Interface for foreign key checkers.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface ForeignKeyChecker {

	/**
	 * <p>Check whether a value exists in a foreign table.</p>
	 * @param tableName String Foreign table name
	 * @param columnName String Foreign column Name
	 * @param columnValue Object Foreign column value
	 * @return boolean
	 * @throws JDOException
	 */
	boolean exists(String tableName, String columnName, Object columnValue) throws JDOException;

}
