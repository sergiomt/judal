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
import java.util.Map;

import javax.jdo.JDOException;
import javax.transaction.TransactionManager;

import org.judal.storage.table.Record;

import com.knowgate.debug.DebugFile;

import org.judal.jdbc.metadata.SQLIndex;
import org.judal.metadata.IndexDef;
import org.judal.storage.relational.RelationalDataSource;

/**
 * Implementation of JDBC relational data source
 * 
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JDBCRelationalDataSource extends JDBCTableDataSource implements RelationalDataSource {

	/**
	 * Constructor
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
	 * {@inheritDoc}
	 */
	@Override
	public JDBCRelationalTable openRelationalTable(Record tableRecord) throws JDOException {
		return openTable(tableRecord);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JDBCRelationalView openRelationalView(Record viewRecord) throws JDOException {
		return new JDBCRelationalView(this, viewRecord);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getRdbmsId() {
		return databaseProductId.intValue();
	}

	@Override
	public SQLIndex createIndexDef(String indexName, String tableName, String[] columns, boolean unique) throws JDOException {
		return new SQLIndex(tableName, indexName, columns, unique);
	}

	@Override
	public void createIndex(IndexDef indexDef) throws JDOException {
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

	@Override
	public void dropIndex(String indexName, String tableName) throws JDOException {
		execute("DROP INDEX " + indexName);
	}

}