package org.judal.bdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.transaction.TransactionManager;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.storage.IndexableTable;
import org.judal.storage.IndexableView;
import org.judal.storage.java.MapRecord;
import org.judal.storage.Record;
import org.judal.storage.TableDataSource;
import org.judal.storage.View;

import com.knowgate.debug.DebugFile;
import com.sleepycat.db.DatabaseException;

public class DBTableDataSource extends DBDataSource implements TableDataSource {

	private SchemaMetaData oSmd;
	
	public DBTableDataSource(Map<String, String> properties, TransactionManager transactManager, SchemaMetaData metaData) throws JDOException {
		super(properties, transactManager, metaData);
		oSmd = metaData;
	}

	// --------------------------------------------------------------------------

	private Properties getTableProperties(String sName, ColumnDef[] oCols) {
		Properties oProps = new Properties();
		oProps.put("name", sName);
		StringBuffer oIndxs = new StringBuffer();
		boolean bFirst = true;
		for (ColumnDef oCol : oCols) {
			if (oCol.isIndexed() && !oCol.isPrimaryKey()) {
				if (!bFirst) {
					oIndxs.append(",");					
				} else {
					bFirst = false;
				}
				oIndxs.append("many-to-one ");
				oIndxs.append(oCol.getName());
			}
		}
		if (!bFirst)
			oProps.put("indexes", oIndxs.toString());
		return oProps;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public DBTable openTable(Record recordInstance) throws JDOException,IllegalArgumentException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.openTable("+recordInstance.getClass().getName()+" "+recordInstance.getTableName()+" )");
			DebugFile.incIdent();
		}

		DBTable retval;
		Properties oProps = getTableProperties(recordInstance.getTableName(), new ColumnDef[0]);
		oProps.put("readonly", isReadOnly() ? "true" : "false");
		retval = (DBTable) openTableOrBucket(oProps, getMetaData().getTable(recordInstance.getTableName()), recordInstance.getClass());

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.openTable()");
		}

		return retval;
	} // openTable

	// --------------------------------------------------------------------------

	@Override
	public View openView(Record recordInstance) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.openView("+recordInstance.getClass().getName()+" "+recordInstance.getTableName()+" )");
			DebugFile.incIdent();
		}

		DBTable retval;
		Properties oProps = getTableProperties(recordInstance.getTableName(), new ColumnDef[0]);
		oProps.put("readonly", "true");
		retval = (DBTable) openTableOrBucket(oProps, getMetaData().getTable(recordInstance.getTableName()), recordInstance.getClass());

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.openView()");
		}

		return retval;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public IndexableTable openIndexedTable(Record recordInstance) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.openIndexedTable("+recordInstance.getClass().getName()+" "+recordInstance.getTableName()+" )");
			DebugFile.incIdent();
		}

		DBTable retval;
		Properties oProps = getTableProperties(recordInstance.getTableName(), recordInstance.columns());
		oProps.put("readonly", isReadOnly() ? "true" : "false");
		retval = (DBTable) openTableOrBucket(oProps, getMetaData().getTable(recordInstance.getTableName()), recordInstance.getClass());

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.openIndexedTable()");
		}

		return retval;
	}
	
	// --------------------------------------------------------------------------
	
	@Override
	public IndexableView openIndexedView(Record recordInstance) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.openIndexedView("+recordInstance.getClass().getName()+" "+recordInstance.getTableName()+" )");
			DebugFile.incIdent();
		}

		DBTable retval;
		Properties oProps = getTableProperties(recordInstance.getTableName(), recordInstance.columns());
		oProps.put("readonly", "true");
		retval =  (DBTable) openTableOrBucket(getTableProperties(recordInstance.getTableName(), recordInstance.columns()), getMetaData().getTable(recordInstance.getTableName()), recordInstance.getClass());

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.openIndexedView()");
		}

		return retval;		
	}
	
	// --------------------------------------------------------------------------

	@Override
	public void createTable(TableDef tableDef, Map<String,Object> options) throws JDOException {
		DBTable oTbl = (DBTable) openTableOrBucket(getTableProperties(tableDef.getName(), tableDef.getColumns()), getMetaData().getTable(tableDef.getName()), MapRecord.class);
	  	oTbl.close();
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
	public TableDef createTableDef(String tableName, Map<String, Object> options) throws JDOException {
		TableDef tblDef = new TableDef(tableName);
		oSmd.addTable(tblDef);
		return tblDef;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public void dropTable(String sDbk, boolean cascade) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTableDataSource.dropTable("+sDbk+", "+String.valueOf(cascade)+")");
			DebugFile.incIdent();
		}
		try {
			Properties oProps = getTableProperties(sDbk, getMetaData().getTable(sDbk).getColumns());
			oProps.put("readonly","false");
			DBTable oTbl = (DBTable) openTableOrBucket(oProps, getMetaData().getTable(sDbk), Record.class);
			for (DBIndex oInd : oTbl.indexes())
				oTbl.dropIndex(oInd.getName());
			oTbl.close();
			if (DebugFile.trace)
				DebugFile.writeln("removeDatabase("+getPath()+sDbk+".db, "+sDbk+")");
			getEnvironment().removeDatabase(getTransaction(), getPath()+sDbk+".db", sDbk);
			File oDbf = new File(getPath()+sDbk+".db");
			if (oDbf.exists()) oDbf.delete();
		} catch (FileNotFoundException | DatabaseException e) {
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
		DBTable tbl = (DBTable) openTableOrBucket(getTableProperties(tableName, getMetaData().getColumns(tableName)), getMetaData().getTable(tableName), MapRecord.class);
		try {
			tbl.truncate();
		} finally {
			if (tbl!=null) tbl.close();
		}
	}

	@Override
	public IndexableView openInnerJoinView(Record recordInstance1, String joinedTableName, Entry<String, String> column)
			throws JDOException {
		throw new JDOUnsupportedOptionException("Berkley DB does not natively supports joins");
	}

	@Override
	public IndexableView openOuterJoinView(Record recordInstance1, String joinedTableName, Entry<String, String> column)
			throws JDOException {
		throw new JDOUnsupportedOptionException("Berkley DB does not natively supports joins");
	}

	@Override
	public IndexableView openInnerJoinView(Record recordInstance1, String joinedTableName,
			Entry<String, String>[] columns) throws JDOException {
		throw new JDOUnsupportedOptionException("Berkley DB does not natively supports joins");
	}

	@Override
	public IndexableView openOuterJoinView(Record recordInstance1, String joinedTableName,
			Entry<String, String>[] columns) throws JDOException {
		throw new JDOUnsupportedOptionException("Berkley DB does not natively supports joins");
	}
	
}
