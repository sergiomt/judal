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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.jdo.JDOException;

import org.judal.metadata.SelectableDef;

import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.Record;
import org.judal.storage.StorageObjectFactory;

/**
 * <p>Iterator over an open SQL ResultSet.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JDBCIterator implements AutoCloseable, Iterator<Stored> {

	protected Constructor<? extends Record> recordConstructor;
	protected SelectableDef tableDef;
	private Class<? extends Record> resultClass;
	private Statement stmt;
	private ResultSet rset;
	private Record nextRow;
	private Boolean isNext;

	/**
	 * <p>Constructor.</p>
	 * @param resultClass Class&lt;? extends Record&gt;
	 * @param tableDef SQLSelectableDef
	 * @param stmt PreparedStatement
	 * @param rset ResultSet
	 * @throws NoSuchMethodException if resultClass has no default constructor nor a constructor compatible with a SelectableDef argument
	 * @throws SecurityException
	 * @throws NullPointerException if resultClass or stmt or rset are null
	 */
	@SuppressWarnings("unchecked")
	public JDBCIterator(Class<? extends Record> resultClass, SelectableDef tableDef, PreparedStatement stmt, ResultSet rset)
		throws NoSuchMethodException, SecurityException, NullPointerException {

		if (null==resultClass)
			throw new NullPointerException("JDBCIterator result class cannot be null");

		if (null==stmt)
			throw new NullPointerException("JDBCIterator Statement is required");
		
		if (null==rset)
			throw new NullPointerException("JDBCIterator ResultSet is required");

		this.stmt = stmt;
		this.rset = rset;
		this.nextRow = null;
		this.resultClass = resultClass;
		this.recordConstructor = (Constructor<? extends Record>) StorageObjectFactory.getConstructor(resultClass, new Class<?>[]{tableDef.getClass()});
		this.isNext = null;

		try {
			if (null==recordConstructor)
				this.recordConstructor = resultClass.getConstructor();
		} catch (Exception ignore) { }

		if (null==recordConstructor)
			throw new NoSuchMethodException("Cannot find a suitable construct for "+resultClass.getName());	
	}

	/**
	 * @return boolean
	 */
	@Override
	public boolean hasNext()  {
		if (null==isNext)
			return moveToNext();
		else if (isNext)
			return true;
		else
			return moveToNext();
	}

	private boolean moveToNext() {
		boolean retval = false;
		try {
			retval = rset.next();
			if (retval) {
				switch (recordConstructor.getParameterCount()) {
					case 0:
						nextRow = recordConstructor.newInstance();
						break;
					case 1:
						nextRow = recordConstructor.newInstance(tableDef);
						break;
					default:
						throw new JDOException("Cannot instantiate "+resultClass.getName()+" from "+tableDef.getClass().getName());
				}
				final int colCount = nextRow.columns().length;
				for (int col=1; col<=colCount; col++) {
					Object value = rset.getObject(col);
					nextRow.put (col, rset.wasNull() ? null : value);
				} // next
			}
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException | SQLException xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
		isNext = retval;
		return retval;		
	}

	/**
	 * @return Record
	 * @throws NoSuchElementException
	 */
	@Override
	public Record next() throws NoSuchElementException {
		if (null==isNext || !isNext)
			if (!moveToNext())
				throw new NoSuchElementException("Attempt to move cursor beyond last record");				
		isNext = Boolean.FALSE;
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
