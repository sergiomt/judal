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


import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.judal.storage.Pair;

import org.judal.storage.table.SchemalessIndexableTable;
import org.judal.storage.table.SchemalessIndexableView;
import org.judal.storage.table.Record;
import org.judal.storage.table.SchemalessTableDataSource;
import org.judal.storage.table.SchemalessView;

import org.judal.metadata.NameAlias;
import org.judal.metadata.JoinType;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;

/**
 * Partial Implementation of TableDataSource interface for HBase
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class HBSchemalessDataSource extends HBBaseDataSource implements SchemalessTableDataSource {

	public HBSchemalessDataSource() {
	}

	public HBSchemalessDataSource(String sPath) throws JDOException {
		super(sPath);
	}

	/**
	 * @param name String Table name
	 * @throws JDOException
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 */
	@SuppressWarnings("deprecation")
	@Override
	public void createTable(String name, Map<String,Object> options) throws JDOException {
		if (null==name)
			throw new NullPointerException("HBSchemalessDataSource.createTable() Table Name cannot be null");
		HTableDescriptor oTds;
		TableName tableName = TableName.valueOf(name);
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
				try (Admin oAdm = oCon.getAdmin()) {
					oAdm.createTable(oTds);
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
	public HBSchemalessTable openTable(Record oRec) throws JDOException {
		HBSchemalessTable oTbl = null;
		try {
			oTbl = new HBSchemalessTable(this, getConfig(), oRec);
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
	public void dropTable(String sName, boolean bCascade) throws JDOException {
		if (bCascade)
			throw new JDOUnsupportedOptionException("HBase does not support drop cascade option");
		try (ClusterConnection oCon = (ClusterConnection) ConnectionFactory.createConnection(getConfig())) {
			TableName tableName = TableName.valueOf(sName);
			try (Admin oAdm = oCon.getAdmin()) {
				oAdm.disableTable(tableName);
				oAdm.deleteTable(tableName);
			}
		} catch (MasterNotRunningException mnre) {
			throw new JDOException("HBTable.truncate() MasterNotRunningException "+mnre.getMessage(), mnre);		  
		} catch (ZooKeeperConnectionException zkce) {
			throw new JDOException("HBTable.truncate() ZooKeeperConnectionException "+zkce.getMessage(), zkce);		  		  
		} catch (IOException ioe) {
			throw new JDOException("HBTable.truncate() IOException "+ioe.getMessage(), ioe);		  
		}
	}

	@Override
	public void truncateTable(String tableName, boolean cascade) throws JDOException {

		try (ClusterConnection oCon = (ClusterConnection) ConnectionFactory.createConnection(getConfig())) {
			TableName tblName = TableName.valueOf(tableName);
			try (Admin oAdm = oCon.getAdmin()) {
				oAdm.disableTable(tblName);
				oAdm.deleteTable(tblName);
			}
			createTable(tableName, new HashMap<String,Object>());
		} catch (MasterNotRunningException mnre) {
			throw new JDOException("HBTable.truncate() MasterNotRunningException "+mnre.getMessage(), mnre);
		} catch (ZooKeeperConnectionException zkce) {
			throw new JDOException("HBTable.truncate() ZooKeeperConnectionException "+zkce.getMessage(), zkce);
		} catch (IOException ioe) {
			throw new JDOException("HBTable.truncate() IOException "+ioe.getMessage(), ioe);
		}
	}

	/**
	 * Secondary indexes are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public SchemalessIndexableTable openIndexedTable(Record recordInstance) throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("HBase does not support tables with secondary indexes");
	}

	@Override
	public SchemalessView openView(Record recordInstance) throws JDOException {
		return openTable(recordInstance);
	}

	/**
	 * Secondary indexes are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public SchemalessIndexableView openIndexedView(Record recordInstance) throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("HBase does not support tables with secondary indexes");
	}

	/**
	 * Inner joins are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@SuppressWarnings("unchecked")
	@Override
	public SchemalessIndexableView openJoinView(JoinType joinType, Record result, NameAlias baseTable, NameAlias joinedTable, Pair<String,String>... onColumns) 
			throws JDOException {
		throw new JDOUnsupportedOptionException("HBase does not support inner join views");
	}

}
