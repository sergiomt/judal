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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.judal.jdbc.jdc.JDCConnection;
import org.judal.jdbc.metadata.SQLColumn;
import org.judal.jdbc.metadata.SQLIndex;
import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.jdbc.metadata.SQLViewDef;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.ForeignKeyDef;
import org.judal.metadata.IndexDef;
import org.judal.metadata.JoinDef;
import org.judal.metadata.JoinType;
import org.judal.metadata.NameAlias;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.storage.DataSource;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;

import com.knowgate.debug.DebugFile;
import com.knowgate.tuples.Pair;

/**
 * <p>Implementation of JDBC table data source</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JDBCTableDataSource extends JDBCBucketDataSource implements TableDataSource {
	
	/**
	 * <p>Constructor</p>
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
	 * <p>Open table for read/write.</p>
	 * Each table uses its own java.sql.Connection.
	 * If this data source is inTransaction() Then the underlying connection will be enlisted in the resources participating in the transaction.
	 * @param recordInstance Record Instance of the Record subclass that will be used to read/write the table
	 * @return JDBCRelationalTable
	 * @throws JDOException
	 */
	@Override
	public JDBCRelationalTable openTable(Record tableRecord) throws JDOException {
		assertNotClosed();
		JDBCRelationalTable tbl = new JDBCRelationalTable(this, tableRecord);
		if (DebugFile.trace)
			DebugFile.writeln("JDBCTableDataSource.openTable(" + tableRecord.getTableName() + ") in transaction "+String.valueOf(inTransaction()));
		if (inTransaction()) {
			try {
				getTransactionManager().getTransaction().enlistResource(tbl.getConnection());
			} catch (IllegalStateException | RollbackException | SystemException e) {
				if (DebugFile.trace)
					DebugFile.writeln(e.getClass().getName()+" at JDBCTableDataSource.openTable("+tableRecord.getTableName()+") "+e.getMessage());
				tbl.close();
				tbl = null;
				throw new JDOException(e.getMessage(), e);
			}
		}
		return tbl;
	}

	/**
	 * <p>Open table for read/write.</p>
	 * The implementation of this method is exactly the same as openTable()
	 * @param recordInstance Record Instance of the Record subclass that will be used to read/write the table
	 * @return JDBCRelationalTable
	 * @throws JDOException
	 */
	@Override
	public JDBCRelationalTable openIndexedTable(Record tableRecord) throws JDOException {
		return openTable(tableRecord);
	}
	
	/**
	 * <p>Open view for read-only.</p>
	 * Each view uses its own java.sql.Connection.
	 * @param recordInstance Record Instance of the Record subclass that will be used to read the view
	 * @return JDBCRelationalView
	 * @throws JDOException
	 */
	@Override
	public JDBCIndexableView openView(Record viewRecord) throws JDOException {
		assertNotClosed();
		return new JDBCIndexableView(this, viewRecord);
	}

	/**
	 * <p>Open indexable view for read-only.</p>
	 * The implementation of this method is exactly the same as openView()
	 * @param recordInstance Record Instance of the Record subclass that will be used to read the relational view
	 * @return JDBCRelationalView
	 * @throws JDOException
	 */
	@Override
	public JDBCIndexableView openIndexedView(Record viewRecord) throws JDOException {
		assertNotClosed();
		return new JDBCIndexableView(this, viewRecord);
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public RelationalView openJoinView(JoinType joinType, Record result, NameAlias baseTable, NameAlias joinedTable, Pair<String,String>... onColumns) throws JDOException {

		assertNotClosed();

		JDBCRelationalView tbl = null;		
		
		if (null==joinType)
			throw new JDOUserException("Join type cannot be null");
		if (null==result)
			throw new JDOUserException("Result record cannot be null");
		if (null==baseTable || baseTable.getName()==null || baseTable.getName().trim().length()==0)
			throw new JDOUserException("Base table cannot be empty");
		if (baseTable.getAlias()==null || baseTable.getAlias().trim().length()==0)
			throw new JDOUserException("Base table must be aliased");
		if (null==joinedTable || joinedTable.getName()==null || joinedTable.getName().trim().length()==0)
			throw new JDOUserException("Joined table cannot be empty");
		if (joinedTable.getAlias()==null || joinedTable.getAlias().trim().length()==0)
			throw new JDOUserException("Joined table must be aliased");
		if (null==onColumns || onColumns.length==0)
			throw new JDOUserException("At least one column pair is required");
		
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCTableDataSource.openJoinView("+joinType.name()+","+result.getTableName()+","+baseTable.getName()+" AS "+baseTable.getAlias()+","+joinedTable.getName()+" AS "+joinedTable.getAlias()+", ...)");
			DebugFile.incIdent();
		}
		
		try {
			tbl = new JDBCRelationalView(this, result);
		} catch (Exception xcpt) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new JDOException(xcpt.getMessage(), xcpt);
		} finally {
			if (tbl!=null) tbl.close();			
		}

		SQLViewDef tdef = tbl.getViewDef().clone();
		tdef.setAlias(baseTable.getAlias());

		ForeignKeyDef fk = null;

		if (DebugFile.trace)
			DebugFile.writeln(String.valueOf(tdef.getNumberOfForeignKeys()) + " foreign keys found");

		for (int f=0; f<tdef.getNumberOfForeignKeys() && fk==null; f++) {
			if (DebugFile.trace)
				DebugFile.writeln("check " + tdef.getForeignKeys()[f].getTable() + " vs " + joinedTable.getName());
			if (tdef.getForeignKeys()[f].getTable().equalsIgnoreCase(joinedTable.getName())) {
				fk = tdef.getForeignKeys()[f];
				if (DebugFile.trace)
					DebugFile.writeln("using foreign key " + joinedTable.getName());
			}
		}
		
		if (null==fk) {
			if (DebugFile.trace) {
				DebugFile.writeln("no suitable foreign key found");
				DebugFile.writeln("set foreign key table " + joinedTable.toString());
			}
			fk = tdef.newForeignKeyMetadata();
			fk.setTable(joinedTable.toString());
			int pos = 0;
			for (Pair<String,String> column : onColumns) {
				try {
					ColumnDef cdef = tdef.getColumnByName(column.$1());
					cdef.setPosition(++pos);
					cdef.setName(column.$2());
					fk.addColumn(cdef);
					if (DebugFile.trace)
						DebugFile.writeln("set foreign key column name " + column.$2());
				} catch (ArrayIndexOutOfBoundsException aiob) {
					if (DebugFile.trace)
						DebugFile.writeln("ArrayIndexOutOfBoundsException " + aiob.getMessage() + " {" + tdef.getColumnsStr() + "}");
					throw new JDOUserException(aiob.getMessage() + " {" + tdef.getColumnsStr() + "}", aiob);
				}
			}
		}

		if (DebugFile.trace)
			DebugFile.writeln(String.valueOf(tdef.getNumberOfJoins()) + " joins found");

		JoinDef jn = null;
		for (int j=0; j<tdef.getNumberOfJoins() && jn==null; j++) {
			if (DebugFile.trace)
				DebugFile.writeln("check " + tdef.getJoins()[j].getTable() + " vs " + joinedTable.getName());
			if (tdef.getJoins()[j].getTable().equalsIgnoreCase(joinedTable.getName())) {
				jn = tdef.getJoins()[j];
			}
		}
		
		if (null==jn) {
			if (DebugFile.trace) {
				DebugFile.writeln("no suitable predefined join found");
				DebugFile.writeln("set join table " + joinedTable.toString());
			}
			jn = tdef.newJoinMetadata();
			jn.setOuter(joinType.equals(JoinType.OUTER));
			jn.setTable(joinedTable.toString());
			for (Pair<String,String> column : onColumns) {
				if (DebugFile.trace)
					DebugFile.writeln("add column " + column.$2());
				jn.addColumn(column.$2());
			}
		}
		
		tbl = new JDBCRelationalView(this, tdef, result.getClass());
		
		if (DebugFile.trace) {
			DebugFile.writeln("joined tables are " + tdef.getTables());
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCTableDataSource.openJoinView()");
		}
		return tbl;
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
		if (null==smd)
			throw new NullPointerException("JDBCTableDataSource.setMetaData() SchemaMetaData cannot be null");
		metaData.clear();
		metaData.addMetadata(smd);
	}

  	/**
	 * Create new SQL column definition
	 * @param tableName String Column Name
	 * @param position int [1..n]
	 * @param colType short java.sql.Types
	 * @param options Map&lt;String,Object&gt; Unused
	 * @return SQLTableDef
	 * @throws JDOException
	 */
	@Override
	public SQLColumn createColumnDef(String columnName, int position, short sqlType, Map<String,Object> options) throws JDOException {
		return new SQLColumn(columnName, position, sqlType);
	}

	// --------------------------------------------------------------------------

	@Override
	public IndexDef createIndexDef(String indexName, String tableName, Iterable<String> columns, IndexDef.Type indexType, IndexDef.Using using) throws JDOException {
		Collection<String> cols = new ArrayList<String>();
		for (String col : columns)
			cols.add(col);
		if (cols.size()==0) {
			throw new JDOException("No columns specified for Index " + indexName + " on table " + tableName);
		} else {
			return new SQLIndex(tableName, indexName, cols.toArray(new String[cols.size()]), IndexDef.Type.ONE_TO_ONE.equals(indexType), using); 
		}
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
			if (!getMetaData().containsTable(tableDef.getName())) {
				cacheTableMetadata((SQLTableDef) tableDef);
				conn = getConnection("JDBCTableDataSource");
				addColumnsToCache((SQLTableDef) tableDef, conn, conn.getMetaData());
				conn.close("JDBCTableDataSource");
				conn = null;
			}
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
		assertNotClosed();
		execute("DROP TABLE "+tableName+(cascade ? " CASCADE" : ""));
		if (getMetaData().containsTable(tableName))
			getMetaData().removeTable(tableName, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void truncateTable(String tableName, boolean cascade) throws JDOException {
		assertNotClosed();
		execute("TRUNCATE TABLE "+tableName+(cascade ? " CASCADE" : ""));
	}
	
}