package org.judal.jdbc.metadata;

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

import java.sql.SQLException;

import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.JDOUserException;

import org.judal.jdbc.jdc.JDCConnection;
import org.judal.metadata.Scriptable;
import org.judal.metadata.SelectableDef;
import org.judal.storage.table.Record;

public interface SQLSelectableDef extends SelectableDef, Scriptable {

	/**
	 * Get table names including joined tables if present
	 * @return String
	 */
	String getTables() throws JDOUserException,JDOUnsupportedOptionException;
	
	/**
	 * <p>Load a single table register into a Record</p>
	 * @param oConn Database Connection
	 * @param PKValues Primary key values of register to be read, in the same order as they appear in table source.
	 * @param AllValues Record Output parameter. Read values.
	 * @return <b>true</b> if register was found <b>false</b> otherwise.
	 * @throws NullPointerException If all objects in PKValues array are null (only debug version)
	 * @throws ArrayIndexOutOfBoundsException if the length of PKValues array does not match the number of primary key columns of the table
	 * @throws SQLException
	 */
	boolean loadRegister(JDCConnection oConn, Object[] PKValues, Record AllValues)
			throws SQLException, NullPointerException, IllegalStateException, ArrayIndexOutOfBoundsException;
	
	/**
	 * <p>Checks if register exists at this table</p>
	 * @param oConn Database Connection
	 * @param sQueryString Register Query String, as a SQL WHERE clause syntax
	 * @return <b>true</b> if register exists, <b>false</b> otherwise.
	 * @throws SQLException
	 */
	boolean existsRegister(JDCConnection oConn, String sQueryString, Object[] oQueryParams) throws SQLException;
	
}