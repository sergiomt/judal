package org.judal.hbase;

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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import org.judal.storage.Pair;

import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.table.View;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.IndexDef;
import org.judal.metadata.NameAlias;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.metadata.ViewDef;
import org.judal.metadata.JoinType;

import java.io.IOException;
import java.io.StringBufferInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.JDOUserException;

/**
 * Partial Implementation of TableDataSource interface for HBase
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class HBTableDataSource extends HBBaseDataSource implements TableDataSource {

	private SchemaMetaData oSmd;

	public HBTableDataSource(SchemaMetaData oMetaData) {
		oSmd = oMetaData;
	}

	public HBTableDataSource(String sPath, SchemaMetaData oMetaData) throws JDOException {
		super(sPath);
		oSmd = oMetaData;
	}

	@Override
	public ColumnDef createColumnDef(String columnName, int position, short colType, Map<String, Object> options) {
		if (options==null)
			throw new NullPointerException("HBTableDataSource.createColumnDef options parameter is required");
		if (!options.containsKey(ColumnDef.OPTION_FAMILY_NAME))
			throw new JDOUserException("HBTableDataSource.createColumnDef family name is required");
		int colLen = options.containsKey(ColumnDef.OPTION_LENGTH) ? Integer.parseInt(options.get(ColumnDef.OPTION_LENGTH).toString()) : ColumnDef.getDefaultPrecision(colType);
		boolean nullable = options.containsKey(ColumnDef.OPTION_NULLABLE) ? Boolean.parseBoolean(options.get(ColumnDef.OPTION_NULLABLE).toString()) : true;
		boolean isPk = options.containsKey(ColumnDef.OPTION_PRIMARYKEY) ? Boolean.parseBoolean(options.get(ColumnDef.OPTION_PRIMARYKEY).toString()) : false;
		return new ColumnDef(position, (String) options.get(ColumnDef.OPTION_FAMILY_NAME), columnName, colType, colLen,
				nullable, null, null, null, options.get(ColumnDef.OPTION_DEFAULT_VALUE), isPk);
	}

	@Override
	public SchemaMetaData getMetaData() throws JDOException {
		return oSmd;
	}

	@Override
	public void setMetaData(SchemaMetaData oSmd) {
		this.oSmd = oSmd;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void createTable(TableDef tableDef, Map<String,Object> options) throws JDOException {
		HTableDescriptor oTds;
		TableName tableName = TableName.valueOf(tableDef.getName());
		try {
			try (ClusterConnection oCon = (ClusterConnection) ConnectionFactory.createConnection(getConfig())) {
				try (Admin oAdm = oCon.getAdmin()) {
					oAdm.getTableDescriptor(tableName);
					throw new JDOException("HBTableDataSource.createTable() Table "+tableName+" already exists");
				}
			}
		} catch (TableNotFoundException tnfe) {
			try (ClusterConnection oCon = (ClusterConnection) ConnectionFactory.createConnection(getConfig())) {
				oTds = new HTableDescriptor(tableName);
				for (ColumnDef oCol : tableDef.getColumns()) {
					String sFamily = oCol.getFamily();
					if (null==sFamily) 
						throw new JDOUserException("Family for column "+oCol.getName()+" cannot be null");
					if (sFamily.length()==0) 
						throw new JDOUserException("Family for column "+oCol.getName()+" cannot be empty");
					if (sFamily.length()==0) sFamily = "default";
					if (!oTds.hasFamily(Bytes.toBytes(oCol.getFamily()))) {
						try {
							// This raises a NoSuchMethodError
							// oTds.addFamily(new HColumnDescriptor(Bytes.toBytes(oCol.getFamily())));
							HColumnDescriptor coldesc = new HColumnDescriptor(Bytes.toBytes(oCol.getFamily()));
							Method addf = oTds.getClass().getMethod("addFamily", new Class[]{HColumnDescriptor.class});
							addf.invoke(oTds, coldesc);
						} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
							throw new JDOException(e.getClass().getName()+" "+e.getMessage()+" probably a compile vs runtime HBase version mistmatch");
						}
					}
				} // next
				try (Admin oAdm = oCon.getAdmin()) {
					oAdm.createTable(oTds);
					oSmd.addTable(tableDef,null);
				} catch (IOException ioe) {
					throw new JDOException(ioe.getClass().getName()+" "+ioe.getMessage(), ioe);
				}
			} catch (IOException ioe) {
				throw new JDOException(ioe.getClass().getName()+" "+ioe.getMessage(), ioe);
			}
		} catch (IOException ioe) {
			throw new JDOException(ioe.getClass().getName()+" "+ioe.getMessage(), ioe);
		}
	}

	@Override
	public HBTable openTable(Record oRec) throws JDOException {
		HBTable oTbl = null;
		try {
			oTbl = new HBTable(this, getConfig(), oRec);
			addTable(oTbl);
			return oTbl;
		} catch (TableNotFoundException tnf) {
			throw new JDOException("TableNotFoundException "+tnf.getMessage(), tnf);
		} catch (IOException ioe) {
			throw new JDOException("IOException "+ioe.getMessage(), ioe);
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
		}
	}

	@Override
	public void truncateTable(String tableName, boolean cascade) throws JDOException {
		TableDef tblDef = getTableDef(tableName);
		try (ClusterConnection oCon = (ClusterConnection) ConnectionFactory.createConnection(getConfig())) {
			TableName tblName = TableName.valueOf(tableName);
			try (Admin oAdm = oCon.getAdmin()) {
				oAdm.disableTable(tblName);
				oAdm.deleteTable(tblName);
			}
			createTable(tblDef, new HashMap<String,Object>());
		} catch (MasterNotRunningException mnre) {
			throw new JDOException("HBTable.truncate() MasterNotRunningException "+mnre.getMessage(), mnre);
		} catch (ZooKeeperConnectionException zkce) {
			throw new JDOException("HBTable.truncate() ZooKeeperConnectionException "+zkce.getMessage(), zkce);
		} catch (IOException ioe) {
			throw new JDOException("HBTable.truncate() IOException "+ioe.getMessage(), ioe);
		}
	}

	@Override
	public TableDef getTableDef(String tableName) throws JDOException {
		return getMetaData().getTable(tableName);
	}
	
	@Override
	public TableDef getTableOrViewDef(String tableName) throws JDOException {
		return getMetaData().getTable(tableName);
	}

	@Override
	public ViewDef getViewDef(String viewName) throws JDOException {
		throw new JDOUserException("HBTableDataSource.getView() HBase does not support views");
	}


	@Override
	public IndexDef createIndexDef(String indexName, String tableName, Iterable<String> columns, IndexDef.Type indexType, IndexDef.Using using) throws JDOException  {
		throw new JDOUserException("HBTableDataSource.gcreateIndexDef() HBase does not support secondary indexes");
	}

	@Override
	public TableDef createTableDef(String tableName, Map<String, Object> options) throws JDOException {
		return new TableDef(tableName);
	}

	@Override
	public IndexableTable openIndexedTable(Record recordInstance) throws JDOException {
		throw new JDOUnsupportedOptionException("HBase does not support tables with secondary indexes");
	}

	@Override
	public View openView(Record recordInstance) throws JDOException {
		return openTable(recordInstance);
	}

	/**
	 * Secondary indexes are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public IndexableView openIndexedView(Record recordInstance) throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("HBase does not support views with secondary indexes");
	}

	/**
	 * Inner joins are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@SuppressWarnings("unchecked")
	@Override
	public IndexableView openJoinView(JoinType joinType, Record result, NameAlias baseTable, NameAlias joinedTable, Pair<String,String>... onColumns) 
			throws JDOException {
		throw new JDOUnsupportedOptionException("HBase does not support inner join views");
	}

}
