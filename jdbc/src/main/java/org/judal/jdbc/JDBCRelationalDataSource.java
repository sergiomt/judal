package org.judal.jdbc;

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

import java.sql.SQLException;
import java.util.Map;

import javax.jdo.JDOException;
import javax.transaction.TransactionManager;

import org.judal.storage.table.Record;

import com.knowgate.debug.DebugFile;

import org.judal.jdbc.metadata.SQLIndex;
import org.judal.metadata.IndexDef;
import org.judal.storage.relational.RelationalDataSource;

/**
 * <p>Implementation of JDBC relational data source.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JDBCRelationalDataSource extends JDBCTableDataSource implements RelationalDataSource {

	/**
	 * <p>Constructor.</p>
	 * @param properties Map&lt;String, String&gt; As listed in DataSource.PropertyNames
	 * @param transactManager TransactionManager
	 * @throws SQLException
	 * @throws NumberFormatException
	 * @throws ClassNotFoundException
	 * @throws NullPointerException
	 * @throws UnsatisfiedLinkError
	 */
	public JDBCRelationalDataSource(Map<String, String> properties, TransactionManager transactManager)
			throws SQLException, NumberFormatException, ClassNotFoundException, NullPointerException,
			UnsatisfiedLinkError {
		super(properties, transactManager);
	}

	/**
	 * <p>Open JDBC relational table for read/write.</p>
	 * Each table uses its own java.sql.Connection.
	 * If this data source is inTransaction() Then the underlying connection will be enlisted in the resources participating in the transaction.
	 * @param tableRecord Record Instance of the Record subclass that will be used to read/write the table
	 * @return JDBCRelationalTable
	 * @throws JDOException
	 */
	@Override
	public JDBCRelationalTable openRelationalTable(Record tableRecord) throws JDOException {
		return openTable(tableRecord);
	}

	/**
	 * <p>Open JDBC relational view for read only.</p>
	 * Each view uses its own java.sql.Connection.
	 * @param viewRecord Record Instance of the Record subclass that will be used to read the view
	 * @return JDBCRelationalView
	 * @throws JDOException
	 */
	@Override
	public JDBCRelationalView openRelationalView(Record viewRecord) throws JDOException {
		assertNotClosed();
		return new JDBCRelationalView(this, viewRecord);
	}

	/**
	 * <p>Open JDBC relational view for read only.</p>
	 * Each view uses its own java.sql.Connection.
	 * @param viewRecord Record Instance of the Record subclass that will be used to read the view
	 * @param alias String Alias to be given to the view when used in a query
	 * @return JDBCRelationalView
	 * @throws JDOException
	 */
	@Override
	public JDBCRelationalView openRelationalView(Record viewRecord, String alias) throws JDOException {
		assertNotClosed();
		JDBCRelationalView view = new JDBCRelationalView(this, viewRecord);
		view.setAlias(alias);
		return view;
	}

	/**
	 * @return int Value of org.judal.jdbc.RDBMS enum
	 */
	@Override
	public int getRdbmsId() {
		return databaseProductId.intValue();
	}

	/**
	 * <p>Create index definition (not the index itself).</p>
	 * @param indexName String
	 * @param tableName String
	 * @param columns String[]
	 * @param unique boolean
	 * @return SQLIndex
	 * @throws JDOException
	 */
	@Override
	public SQLIndex createIndexDef(String indexName, String tableName, String[] columns, boolean unique) throws JDOException {
		try {
			return JDBCMetadataObjectFactory.newIndexDef(RDBMS.valueOf(getRdbmsId()), tableName, indexName, columns, unique);
		} catch (NoSuchMethodException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	/**
	 * <p>Create index in the database.</p>
	 * @param indexDef IndexDef
	 * @throws JDOException
	 */
	@Override
	public void createIndex(IndexDef indexDef) throws JDOException {
		assertNotClosed();
		try {
			if (DebugFile.trace) {
				DebugFile.writeln("Begin JDBCRelationalDataSource.createIndex("+indexDef.getName()+")");
				DebugFile.incIdent();
			}
			String ddl = ((SQLIndex) indexDef).sqlScriptDef(getDatabaseProductId());
			execute(ddl);
			if (DebugFile.trace) {
				DebugFile.writeln("End JDBCRelationalDataSource.createIndex("+indexDef.getName()+")");
				DebugFile.decIdent();
			}
		} catch (SQLException sqle) {
			if (DebugFile.trace) {
				DebugFile.writeln("SQLException "+sqle.getMessage());
				DebugFile.decIdent();
			}
		}
	}

	/**
	 * <p>Drop index from the database.</p>
	 * @param indexName String
	 * @param tableName String
	 * @throws JDOException
	 */
	@Override
	public void dropIndex(String indexName, String tableName) throws JDOException {
		assertNotClosed();
		execute("DROP INDEX " + indexName);
	}

}