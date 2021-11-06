package org.judal.bdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.JDOUserException;
import javax.jdo.metadata.ColumnMetadata;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.IndexDef;
import org.judal.metadata.JoinType;
import org.judal.metadata.NameAlias;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.metadata.ViewDef;
import org.judal.storage.FieldHelper;
import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.table.View;

import com.knowgate.debug.DebugFile;
import com.knowgate.tuples.Pair;

import com.sleepycat.db.DatabaseException;

public class DBTableDataSource extends DBBucketDataSource implements TableDataSource {

	private SchemaMetaData oSmd;
	
	public DBTableDataSource(Map<String, String> properties, TransactionManager transactManager, SchemaMetaData metaData) throws JDOException {
		super(properties, transactManager, metaData);
		oSmd = metaData;
		initializeTables();
	}

	// --------------------------------------------------------------------------

	private Properties getTableProperties(TableDef tdef) {
		Properties oProps = new Properties();
		oProps.put("name", tdef.getName());
		LinkedList<String> indexedColumns = new LinkedList<String>();
		for (ColumnDef oCol : tdef.getColumns()) {
			if (oCol.isIndexed() && !oCol.isPrimaryKey()) {
				indexedColumns.add("many-to-one " + oCol.getName());
			}
		}
		for (IndexDef index : tdef.getIndices()) {
			for (ColumnMetadata cmeta : index.getColumns()) {
				if (!tdef.getColumnByName(cmeta.getName()).isPrimaryKey() && !indexedColumns.contains("many-to-one " + cmeta.getName()))
					indexedColumns.add("many-to-one " + cmeta.getName());
			}
		}

		if (indexedColumns.size()>0) {
			if (DebugFile.trace)
				DebugFile.writeln("table " + tdef.getName() + " has indexed columns [" + String.join(",", indexedColumns) + "]");
			oProps.put("indexes", String.join(",", indexedColumns));
		} else {
			if (DebugFile.trace)
				DebugFile.writeln("table " + tdef.getName() + " has no secondary indexes");
		}
		return oProps;
	}

	// --------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private void initializeTables() {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.initializeTables()");
			DebugFile.incIdent();
		}

		if (null!=oSmd) {
			for (TableDef tdef : oSmd.tables()) {
				final String tblName = tdef.getName();
				Properties oProps = getTableProperties(tdef);
				oProps.put("readonly", "false");
				DBBucket table = null;
				try {
					table = openTableOrBucket(oProps, getMetaData().getTable(tblName), tdef.getRecordClass(), true);
				} catch (JDOException | IllegalArgumentException | IllegalStateException | ClassNotFoundException e) {
					if (DebugFile.trace)
						DebugFile.writeln(e.getClass().getName() + " " + e.getMessage());
				} finally {
					if (table!=null)
						table.close();
				}
			}
		}
		
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTableDataSource.initializeTables()");
		}
	}


	// --------------------------------------------------------------------------
	
	@Override
	public DBTable openTable(Record recordInstance) throws JDOException,IllegalArgumentException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTableDataSource.openTable("+recordInstance.getClass().getName()+" "+recordInstance.getTableName()+" )");
			DebugFile.incIdent();
		}

		DBTable retval;
		TableDef tdef = oSmd.getTable(recordInstance.getTableName());
		if (null==tdef)
			throw new JDOException("Table " + recordInstance.getTableName() + " not found in schema metadata");
		Properties oProps = getTableProperties(tdef);
		oProps.put("readonly", isReadOnly() ? "true" : "false");
		retval = (DBTable) openTableOrBucket(oProps, getMetaData().getTable(recordInstance.getTableName()), recordInstance.getClass(), true);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTableDataSource.openTable()");
		}

		return retval;
	} // openTable

	// --------------------------------------------------------------------------

	@Override
	public View openView(Record recordInstance) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTableDataSource.openView("+recordInstance.getClass().getName()+" "+recordInstance.getTableName()+" )");
			DebugFile.incIdent();
		}

		DBTable retval;
		Properties oProps = getTableProperties(oSmd.getTable(recordInstance.getTableName()));
		oProps.put("readonly", "true");
		retval = (DBTable) openTableOrBucket(oProps, getMetaData().getTable(recordInstance.getTableName()), recordInstance.getClass(), true);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTableDataSource.openView()");
		}

		return retval;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public IndexableTable openIndexedTable(Record recordInstance) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTableDataSource.openIndexedTable("+recordInstance.getClass().getName()+" "+recordInstance.getTableName()+" )");
			DebugFile.incIdent();
		}

		DBTable retval;
		final String tblName = recordInstance.getTableName();
		Properties oProps = getTableProperties(oSmd.getTable(recordInstance.getTableName()));
		oProps.put("readonly", isReadOnly() ? "true" : "false");
		retval = (DBTable) openTableOrBucket(oProps, getMetaData().getTable(tblName), recordInstance.getClass(), true);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTableDataSource.openIndexedTable()");
		}

		return retval;
	}
	
	// --------------------------------------------------------------------------
	
	@Override
	public IndexableView openIndexedView(Record recordInstance) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTableDataSource.openIndexedView("+recordInstance.getClass().getName()+" "+recordInstance.getTableName()+" )");
			DebugFile.incIdent();
		}

		DBTable retval;
		final String viewName = recordInstance.getTableName();
		Properties oProps = getTableProperties(oSmd.getTable(recordInstance.getTableName()));
		oProps.put("readonly", "true");
		retval =  (DBTable) openTableOrBucket(oProps, getMetaData().getTable(recordInstance.getTableName()), recordInstance.getClass(), true);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTableDataSource.openIndexedView()");
		}

		return retval;		
	}
	
	// --------------------------------------------------------------------------

	@Override
	public void createTable(TableDef tableDef, Map<String,Object> options) throws JDOException {
		if (inTransaction())
			throw new JDOException("Cannot create a table in the middle of a transaction");
		try {
			if (isTransactional())
				getTransactionManager().begin();
			DBTable oTbl = (DBTable) openTableOrBucket(getTableProperties(tableDef), tableDef, tableDef.getRecordClass(), isTransactional());
			if (isTransactional())
				getTransactionManager().commit();
			oTbl.close();
		} catch (NotSupportedException | SystemException | SecurityException | IllegalStateException | RollbackException | HeuristicMixedException | HeuristicRollbackException | IllegalArgumentException | ClassNotFoundException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	// --------------------------------------------------------------------------

	@Override
	public FieldHelper getFieldHelper() throws JDOException {
		return null;
	}

	// --------------------------------------------------------------------------

	@Override
	public SchemaMetaData getMetaData() throws JDOException {
		return oSmd;
	}

	// --------------------------------------------------------------------------

	@Override
	public void setMetaData(SchemaMetaData oSmd) throws JDOException {
		this.oSmd = oSmd;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public TableDef getTableDef(String tableName) throws JDOException {
		return oSmd.getTable(tableName);
	}

	// --------------------------------------------------------------------------
	
	@Override
	public ViewDef getViewDef(String viewName) throws JDOException {
		throw new JDOUserException("DBTableDataSource.getViewDef() Berkeley DB does not support views");
	}

	// --------------------------------------------------------------------------
	
	@Override
	public TableDef getTableOrViewDef(String tableName) throws JDOException {
		return oSmd.getTable(tableName);
	}

	// --------------------------------------------------------------------------

	@Override
	public DBIndex createIndexDef(String indexName, String tableName, Iterable<String> columns, IndexDef.Type indexType, IndexDef.Using using) throws JDOException {
		Iterator<String> cols = columns.iterator();
		if (cols.hasNext()) {
			String colName = cols.next();
			if (cols.hasNext())
				throw new JDOException("Berkeley DB does not support indexes on multiple columns");
			else
				return new DBIndex(tableName, colName, indexType);
		} else {
			throw new JDOException("No columns specified for Index " + indexName + " on table " + tableName);
		}
	}

	// --------------------------------------------------------------------------
	
	@Override
	public TableDef createTableDef(String tableName, Map<String, Object> options) throws JDOException {
		TableDef tblDef = new TableDef(tableName);
		oSmd.addTable(tblDef,null);
		return tblDef;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public void dropTable(String sDbk, boolean cascade) throws JDOException {
		if (inTransaction())
			throw new JDOException("Cannot drop a table in the middle of a transaction");
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTableDataSource.dropTable("+sDbk+", "+String.valueOf(cascade)+")");
			DebugFile.incIdent();
		}
		try {
			Properties oProps = getTableProperties(oSmd.getTable(sDbk));
			oProps.put("readonly","false");
			DBTable oTbl = null;
			if (isTransactional())
				getTransactionManager().begin();
			oTbl = (DBTable) openTableOrBucket(oProps, getMetaData().getTable(sDbk), getMetaData().getTable(sDbk).getRecordClass(), isTransactional());
			for (DBIndex oInd : oTbl.indexes())
				oTbl.dropIndex(oInd.getName());
			oTbl.close();
			if (DebugFile.trace)
				DebugFile.writeln("removeDatabase("+getPath()+sDbk+".db, "+sDbk+")");
			getEnvironment().removeDatabase(getTransaction(), getPath()+sDbk+".db", sDbk);
			if (DebugFile.trace)
				DebugFile.writeln("database "+getPath()+sDbk+".db, removed");
			if (isTransactional())
				getTransactionManager().commit();
			File oDbf = new File(getPath()+sDbk+".db");
			if (oDbf.exists()) {
				if (DebugFile.trace) DebugFile.writeln("deleting file "+getPath()+sDbk+".db");
				oDbf.delete();
			}
		} catch (FileNotFoundException | DatabaseException | IllegalArgumentException | IllegalStateException | ClassNotFoundException | NotSupportedException | SystemException | SecurityException | RollbackException | HeuristicMixedException | HeuristicRollbackException e) {
			throw new JDOException(e.getMessage(), e);
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTableDataSource.dropTable()");
		}
	}

	// --------------------------------------------------------------------------
	
	@Override
	public void truncateTable(String tableName, boolean cascade) throws JDOException {
		if (inTransaction())
			throw new JDOException("Cannot truncate a table in the middle of a transaction");

		if (isTransactional())
			try {
				getTransactionManager().begin();
			} catch (NotSupportedException | SystemException e) {
				throw new JDOException(e.getMessage(), e);
			}

		TableDef tdef = getMetaData().getTable(tableName);
		DBTable tbl = null;
		try {
			tbl = (DBTable) openTableOrBucket(getTableProperties(tdef), tdef, tdef.getRecordClass(), isTransactional());
			tbl.truncate(isTransactional());
			if (isTransactional())
				getTransactionManager().commit();
		} catch (SecurityException | IllegalStateException | RollbackException | HeuristicMixedException | HeuristicRollbackException | SystemException | ArrayIndexOutOfBoundsException | IllegalArgumentException | ClassNotFoundException e) {
			throw new JDOException(e.getMessage(), e);
		} finally {
			if (tbl!=null) tbl.close();
		}
	}

	// --------------------------------------------------------------------------

	@Override
	public IndexableView openJoinView(JoinType joinType, Record result, NameAlias baseTable, NameAlias joinedTable,
			Pair<String, String>... onColumns) throws JDOException {
		throw new JDOUnsupportedOptionException("Berkley DB does not natively supports joins");
	}

	// --------------------------------------------------------------------------

	@Override
	public ColumnDef createColumnDef(String columnName, int position, short colType, Map<String, Object> options)
			throws JDOException {
		if (options==null) {
			return new ColumnDef(columnName, position, colType);
		} else {
			int colLen = options.containsKey(ColumnDef.OPTION_LENGTH) ? Integer.parseInt(options.get(ColumnDef.OPTION_LENGTH).toString()) : ColumnDef.getDefaultPrecision(colType);
			boolean nullable = options.containsKey(ColumnDef.OPTION_NULLABLE) ? Boolean.parseBoolean(options.get(ColumnDef.OPTION_NULLABLE).toString()) : true;
			boolean isPk = options.containsKey(ColumnDef.OPTION_PRIMARYKEY) ? Boolean.parseBoolean(options.get(ColumnDef.OPTION_PRIMARYKEY).toString()) : false;
			return new ColumnDef(position, "", columnName, colType, colLen, nullable, null, null, null, options.get(ColumnDef.OPTION_DEFAULT_VALUE), isPk);			
		}
	}
	
}
