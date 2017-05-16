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
import java.util.Map.Entry;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.judal.jdbc.jdc.JDCConnection;
import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.ForeignKeyDef;
import org.judal.metadata.JoinDef;
import org.judal.metadata.NameAlias;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.storage.DataSource;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;

import com.knowgate.debug.DebugFile;

/**
 * Implementation of JDBC table data source
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JDBCTableDataSource extends JDBCBucketDataSource implements TableDataSource {
	
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
	public JDBCTableDataSource(Map<String, String> properties, TransactionManager transactManager) throws SQLException, NumberFormatException,
			ClassNotFoundException, NullPointerException, UnsatisfiedLinkError {
		super(properties,transactManager);
	}

	/**
	 * Open relational table for read/write.
	 * @param recordInstance Record Instance of the Record subclass that will be used to read/write the relational table
	 * @return JDBCRelationalTable
	 * @throws JDOException
	 */
	@Override
	public JDBCRelationalTable openTable(Record tableRecord) throws JDOException {
		JDBCRelationalTable tbl = new JDBCRelationalTable(this, tableRecord);
		if (DebugFile.trace)
			DebugFile.writeln("JDBCTableDataSource.openTable(" + tableRecord.getTableName() + ") in transaction "+String.valueOf(inTransaction()));
		if (inTransaction()) {
			try {
				getTransactionManager().getTransaction().enlistResource(tbl.getConnection());
			} catch (IllegalStateException | RollbackException | SystemException e) {
				tbl.close();
				tbl = null;
				throw new JDOException(e.getMessage(), e);
			}
		}
		return tbl;
	}

	/**
	 * Open relational table for read/write.
	 * @param recordInstance Record Instance of the Record subclass that will be used to read/write the relational table
	 * @return JDBCRelationalTable
	 * @throws JDOException
	 */
	@Override
	public JDBCRelationalTable openIndexedTable(Record tableRecord) throws JDOException {
		return openTable(tableRecord);
	}
	
	/**
	 * Open relational view for read-only.
	 * @param recordInstance Record Instance of the Record subclass that will be used to read the relational view
	 * @return JDBCRelationalView
	 * @throws JDOException
	 */
	@Override
	public JDBCRelationalView openView(Record viewRecord) throws JDOException {
		return new JDBCRelationalView(this, viewRecord);
	}

	/**
	 * Open relational view for read-only.
	 * @param recordInstance Record Instance of the Record subclass that will be used to read the relational view
	 * @return JDBCRelationalView
	 * @throws JDOException
	 */
	@Override
	public JDBCRelationalView openIndexedView(Record viewRecord) throws JDOException {
		return new JDBCRelationalView(this, viewRecord);
	}

	@SuppressWarnings("unchecked")
	@Override
	public JDBCRelationalView openInnerJoinView(Record tableRecord, String joinedTableName, Entry<String,String> column) throws JDOException {
		return openJoinView(tableRecord, joinedTableName, false, new Entry[]{column});
	}

	@SuppressWarnings("unchecked")
	@Override
	public JDBCRelationalView openOuterJoinView(Record tableRecord, String joinedTableName, Entry<String,String> column) throws JDOException {
		return openJoinView(tableRecord, joinedTableName, true, new Entry[]{column});
	}
	
	@Override
	public JDBCRelationalView openInnerJoinView(Record tableRecord, String joinedTableName, Entry<String,String>[] columns) throws JDOException {
		return openJoinView(tableRecord, joinedTableName, false, columns);
	}

	@Override
	public JDBCRelationalView openOuterJoinView(Record tableRecord, String joinedTableName, Entry<String,String>[] columns) throws JDOException {
		return openJoinView(tableRecord, joinedTableName, true, columns);
	}

	private JDBCRelationalView openJoinView(Record tableRecord, String joinedTableName, boolean outer, Entry<String,String>[] columns) throws JDOException {
		JDBCRelationalView tbl = null;		
		try {
			tbl = new JDBCRelationalView(this, tableRecord);
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		} finally {
			if (tbl!=null) tbl.close();			
		}
		SQLTableDef tdef = tbl.getTableDef().clone();
		ForeignKeyDef fk = null;
		NameAlias joinedTableAlias = NameAlias.parse(joinedTableName);
		for (int f=0; f<tdef.getNumberOfForeignKeys() && fk==null; f++) 
			if (tdef.getForeignKeys()[f].getTable().equalsIgnoreCase(joinedTableAlias.getName()))
				fk = tdef.getForeignKeys()[f];
		if (null==fk) {
			fk = tdef.newForeignKeyMetadata();
			fk.setTable(joinedTableName);
			int pos = 0;
			for (Map.Entry<String,String> column : columns) {
				try {
					ColumnDef cdef = tdef.getColumnByName(column.getKey());
					cdef.setPosition(++pos);
					cdef.setName(column.getValue());
					fk.addColumn(cdef);
				} catch (ArrayIndexOutOfBoundsException aiob) {
					throw new JDOUserException(aiob.getMessage(), aiob);
				}
			}
		}
		JoinDef jn = null;
		for (int j=0; j<tdef.getNumberOfJoins() && jn==null; j++)
			if (tdef.getJoins()[j].getTable().equalsIgnoreCase(joinedTableAlias.getName()))
				jn = tdef.getJoins()[j];
		if (null==jn) {
			jn = tdef.newJoinMetadata();
			jn.setOuter(outer);
			jn.setTable(joinedTableName);
			for (Map.Entry<String,String> column : columns)
				jn.addColumn(column.getValue());
		}
		if (jn.getOuter()!=outer)
			throw new JDOUserException("View join was defined as "+(jn.getOuter() ? "OUTER" : "INNER")+" but opened as "+(outer ? "OUTER" : "INNER"));
		return new JDBCRelationalView(this, tdef);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SchemaMetaData getMetaData() {
		return metaData;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setMetaData(SchemaMetaData smd) {
		metaData = smd;
	}

	
	/**
	 * Create new SQL table definition
	 * @param tableName String Table Name
	 * @param options Map&lt;String,Object&gt; May include DataSource.CATALOG and DataSource.SCHEMA
	 * @return SQLTableDef
	 * @throws JDOException
	 */
	@Override
	public SQLTableDef createTableDef(String tableName, Map<String,Object> options) throws JDOException {
		SQLTableDef tableDef;
		try {
			tableDef = new SQLTableDef(getDatabaseProductId(), tableName);
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
		if (options!=null) {
			if (options.containsKey(DataSource.CATALOG))
				tableDef.setCatalog((String) options.get(DataSource.CATALOG));
			if (options.containsKey(DataSource.SCHEMA))
				tableDef.setSchema((String) options.get(DataSource.SCHEMA));
		}
		return tableDef;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createTable(TableDef tableDef, Map<String, Object> options) throws JDOException {
		JDCConnection conn = null;
		try {
			if (DebugFile.trace) {
				DebugFile.writeln("Begin JDBCTableDataSource.createTable("+tableDef.getName()+")");
				DebugFile.incIdent();
			}
			String ddl = ((SQLTableDef) tableDef).getSource();
			execute(ddl);
			addTableToCache((SQLTableDef) tableDef);
			conn = getConnection("JDBCTableDataSource");
			addColumnsToCache((SQLTableDef) tableDef, conn, conn.getMetaData());
			conn.close("JDBCTableDataSource");
			conn = null;
			if (DebugFile.trace) {
				DebugFile.writeln("End JDBCTableDataSource.createTable("+tableDef.getName()+")");
				DebugFile.decIdent();
			}
		} catch (SQLException sqle) {
			try { if (conn!=null) conn.close("JDBCTableDataSource"); } catch (Exception ignore) { }
			if (DebugFile.trace) {
				DebugFile.writeln("SQLException "+sqle.getMessage());
				DebugFile.decIdent();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void dropTable(String tableName, boolean cascade) throws JDOException {
		execute("DROP TABLE "+tableName+(cascade ? " CASCADE" : ""));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void truncateTable(String tableName, boolean cascade) throws JDOException {
		execute("TRUNCATE TABLE "+tableName+(cascade ? " CASCADE" : ""));
	}
	
}