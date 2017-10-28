package org.judal.jdbc;

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

import javax.jdo.JDOException;

import org.judal.jdbc.jdc.JDCConnection;

/**
 * <p>Abstract base class for JDBC buckets, views and tables.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public abstract class JDBCBase implements AutoCloseable {

	protected JDCConnection jdcConn;
	protected String connName;
	
	/**
	 * <p>Constructor.</p>
	 * Grab one JDBC connection from the pool to be used by a view or table subclass.
	 * @param dataSource JDBCDataSource
	 * @param connName String
	 * @throws JDOException
	 */
	public JDBCBase(JDBCDataSource dataSource, String connName) throws JDOException {
		this.connName = connName;
		try {
			jdcConn = dataSource.getConnection(connName);
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}		
	}

	/**
	 * @return JDCConnection
	 * @see org.judal.jdbc.jdc.JDCConnection
	 */
	public JDCConnection getConnection() {
		return jdcConn;
	}

	/**
	 * <p>Return the internal JDBC connection to the pool.</p>
	 * @throws JDOException
	 */
	@Override
	public void close() throws JDOException {
		try {
			jdcConn.close(connName);
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}
	
}