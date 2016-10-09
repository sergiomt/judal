package org.judal.jdbc;

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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Iterator;

import javax.jdo.JDOException;

import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.storage.Record;
import org.judal.storage.Stored;

public class JDBCIterator implements AutoCloseable, Iterator<Stored> {

	protected Constructor<? extends Record> recordConstructor;
	protected SQLTableDef tableDef;
	private Class<? extends Record> resultClass;
	private Statement stmt;
	private ResultSet rset;
	private Record nextRow;

	public JDBCIterator(Class resultClass, SQLTableDef tableDef, PreparedStatement stmt, ResultSet rset)
		throws NoSuchMethodException, SecurityException {
		this.stmt = stmt;
		this.rset = rset;
		this.nextRow = null;
		this.resultClass = resultClass;
		this.recordConstructor = this.resultClass.getConstructor(tableDef.getClass());
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean hasNext() {
		boolean retval = false;
		try {
			retval = rset.next();
			if (retval) {
				nextRow = recordConstructor.newInstance(tableDef);
				final int colCount = nextRow.columns().length;
				for (int col=1; col<=colCount; col++) {
					Object value = rset.getObject(col);
					nextRow.put (col, rset.wasNull() ? null : value);
				} // next
			}
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException | SQLException xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
		return retval;
	}

	@Override
	public Stored next() {
		return nextRow;
	}

	@Override
	public void close() {
		try {
			if (rset!=null) {
				rset.close();
				rset = null;
			}
		} catch (SQLException ignore) { }
		try {
			if (stmt!=null) {
				stmt.close();
				stmt = null;
			}
		} catch (SQLException ignore) { }
	}
}
