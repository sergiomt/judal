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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.VersionInfo;

import com.knowgate.debug.DebugFile;
import com.knowgate.io.FileUtils;

import org.judal.storage.FieldHelper;
import org.judal.storage.Param;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.IndexDef;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringBufferInputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.JDOUserException;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;

import javax.transaction.TransactionManager;

/**
 * Partial Implementation of TableDataSource interface for HBase
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public abstract class HBBaseDataSource {

	// A constant to convert a fraction to a percentage
	private static final int CONVERT_TO_PERCENTAGE = 100;

	@SuppressWarnings("deprecation")
	private StringBufferInputStream oInStrm1, oInStrm2;

	private Configuration oCfg;

	private final Set<HBSchemalessTable> oOTbls;

	private FieldHelper ofldHlpr;

	public HBBaseDataSource() {
		oOTbls = Collections.synchronizedSet(new HashSet<HBSchemalessTable>());
		oInStrm1 = oInStrm2 = null;
		ofldHlpr = new HBDefaultFieldHelper();
	}

	public HBBaseDataSource(String sPath) throws JDOException {
		oOTbls = Collections.synchronizedSet(new HashSet<HBSchemalessTable>());
		open(sPath,null,null,false);
	}

	public Configuration getConfig() {
		if (oInStrm1!=null) oInStrm1.reset();
		if (oInStrm2!=null) oInStrm2.reset();
		return oCfg;  
	}

	private void checkDefaultsVersion() throws IOException {
		if (DebugFile.trace) DebugFile.writeln("getting hbase.defaults.for.version.skip");
		if (getConfig().getBoolean("hbase.defaults.for.version.skip", Boolean.FALSE)) return;
		if (DebugFile.trace) DebugFile.writeln("getting hbase.defaults.for.version");
		String defaultsVersion = getConfig().get("hbase.defaults.for.version");
		String thisVersion = VersionInfo.getVersion();
		if (DebugFile.trace) DebugFile.writeln("this version is " + thisVersion);
		if (null!=thisVersion)
			if (!thisVersion.equals(defaultsVersion))
				throw new IOException("hbase-default.xml file seems to be for and old version of HBase (" + defaultsVersion + "), this version is " + thisVersion);
		if (DebugFile.trace) DebugFile.writeln("finished checkDefaultsVersion");
	}

	private void checkForClusterFreeMemoryLimit() throws IOException {
		if (DebugFile.trace) DebugFile.writeln("getting hbase.regionserver.global.memstore.upperLimit");
		float globalMemstoreLimit = getConfig().getFloat("hbase.regionserver.global.memstore.upperLimit", 0.4f);
		int gml = (int)(globalMemstoreLimit * CONVERT_TO_PERCENTAGE);
		if (DebugFile.trace) DebugFile.writeln("getting "+HConstants.HFILE_BLOCK_CACHE_SIZE_KEY);
		float blockCacheUpperLimit = getConfig().getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY,HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT);
		int bcul = (int)(blockCacheUpperLimit * CONVERT_TO_PERCENTAGE);
		if (CONVERT_TO_PERCENTAGE - (gml + bcul)
				< (int)(CONVERT_TO_PERCENTAGE * 
						HConstants.HBASE_CLUSTER_MINIMUM_MEMORY_THRESHOLD)) {
			throw new IOException(
					"Current heap configuration for MemStore and BlockCache exceeds " +
							"the threshold required for successful cluster operation. " +
							"The combined value cannot exceed 0.8. Please check " +
							"the settings for hbase.regionserver.global.memstore.upperLimit and " +
							"hfile.block.cache.size in your configuration. " +
							"hbase.regionserver.global.memstore.upperLimit is " + 
							globalMemstoreLimit +
							" hfile.block.cache.size is " + blockCacheUpperLimit);
		}
		if (DebugFile.trace) DebugFile.writeln("finished checkForClusterFreeMemoryLimit");
	}

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

	/**
	 * 
	 * @return whether to show HBase Configuration in servlet
	 */
	public static boolean isShowConfInServlet() {
		boolean isShowConf = false;
		try {
			if (Class.forName("org.apache.hadoop.conf.ConfServlet") != null) {
				isShowConf = true;  
			}
		} catch (Exception e) {

		}
		return isShowConf;
	}

	public void open(String sPath, String sUser, String sPassw, boolean bReadOnly)
			throws JDOException, NullPointerException {

		if (null==sPath) throw new NullPointerException("HBConfig.open() path cannot be null");
		if (sPath.length()==0) throw new NullPointerException("HBConfig.open() path cannot be empty");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin HBBaseDataSource.open(" + sPath + ")");
			DebugFile.incIdent();
		}

		try {
			oCfg = new Configuration();

			if (!sPath.endsWith(File.separator)) sPath += File.separator;

			File oFle = new File(sPath+"hbase-default.xml");
			if (oFle.exists()) {
				if (DebugFile.trace) DebugFile.writeln("parsing hbase-default.xml");

				String sDefaults = FileUtils.readFileToString(new File(sPath+"hbase-default.xml"), "ISO8859_1");
				oInStrm1 = new StringBufferInputStream(sDefaults);
				oCfg.addResource(oInStrm1);
				if (DebugFile.trace) DebugFile.writeln("checkDefaultsVersion");
				checkDefaultsVersion();
				oInStrm1.reset();
			} else {
				oInStrm1 = null;
			}
			if (DebugFile.trace) DebugFile.writeln("opening "+sPath+"hbase-site.xml");
			oFle = new File(sPath+"hbase-site.xml");
			if (oFle.exists()) {
				if (DebugFile.trace) DebugFile.writeln("parsing hbase-site.xml");
				String sConfig = FileUtils.readFileToString(new File(sPath+"hbase-site.xml"), "ISO8859_1");
				oInStrm2 = new StringBufferInputStream(new String(sConfig));
				oCfg.addResource(oInStrm2);
				if (DebugFile.trace) DebugFile.writeln("checkForClusterFreeMemoryLimit");
				checkForClusterFreeMemoryLimit();
				if (DebugFile.trace) {
					DebugFile.writeln("quorum="+oCfg.get("hbase.zookeeper.quorum"));
					DebugFile.writeln("rootdir="+oCfg.get("hbase.rootdir"));
				}
			} else {
				oInStrm2 = null;
				throw new JDOException("File not found "+sPath+"hbase-site.xml");
			} 
		} catch (FileNotFoundException fnfe) {
			if (DebugFile.trace) {
			  DebugFile.decIdent();
			  DebugFile.writeln("HBConfig.open() FileNotFoundException "+fnfe.getMessage());
		  }
			throw new JDOException(fnfe.getMessage(), fnfe);
		} catch (IOException ioe) {
			if (DebugFile.trace) {
			  DebugFile.decIdent();
			  DebugFile.writeln("HBConfig.open() IOException "+ioe.getMessage());
		  }
			throw new JDOException(ioe.getMessage(), ioe);
		} catch (Exception xcpt) {
			if (DebugFile.trace) {
			  DebugFile.decIdent();
			  DebugFile.writeln("HBConfig.open() "+xcpt.getClass().getName()+" "+xcpt.getMessage());
		  }
			throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End HBBaseDataSource.open()");
		}
	}

	public void closeTables() throws JDOException {
		Iterator<HBSchemalessTable> oIter = oOTbls.iterator();
		try {
			while (oIter.hasNext()) oIter.next().getTable().close();
		} catch (IOException ioe) {
			throw new JDOException(ioe.getMessage(), ioe);  
		}
		oOTbls.clear();	
	}

	public void close() throws JDOException {
		closeTables();
		try {
			if (oInStrm1!=null) oInStrm1.close();
			if (oInStrm2!=null) oInStrm2.close();
		} catch (IOException ignore) { }
		oCfg = null;
	}

	public FieldHelper getFieldHelper() throws JDOException {
		return ofldHlpr;
	}

	public void setFieldHelper(FieldHelper fieldHelper) throws JDOException {
		ofldHlpr = fieldHelper;
	}

	public Set<HBSchemalessTable> openedTables()  {
		return oOTbls;
	}

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

	public boolean exists(String objectName, String objectType) throws JDOException {
		boolean objExists;
		if (!objectType.equals("U"))
			throw new JDOUnsupportedOptionException("HBase only supports type U (table) exists check");
		try (ClusterConnection oCon = (ClusterConnection) ConnectionFactory.createConnection(getConfig())) {
			try (Admin oAdm = oCon.getAdmin()) {
				if (DebugFile.trace) DebugFile.writeln("HBaseAdmin.getTableDescriptor("+objectName+")");
				oAdm.getTableDescriptor(TableName.valueOf(objectName));
				objExists = true;
			}
		} catch (TableNotFoundException tnfe) {
			objExists = false;
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
		}
		return objExists;
	}

	public Map<String, String> getProperties() {
		return null;
	}

	/**
	 * Transactions are not supported by HBase. Therefore this method will always return <b>null</b>.
	 * @return <b>null</b>
	 */
	public TransactionManager getTransactionManager() {
		return null;
	}

	/**
	 * @return <b>null</b>
	 */
	public JDOConnection getJdoConnection() throws JDOException {
		return null;
	}

	/**
	 * Sequences are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	public Sequence getSequence(String name) throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("HBase does not support sequences");
	}

	/**
	 * Transactions are not supported by HBase. Therefore this method will always return <b>false</b>.
	 * @return boolean <b>false</b>
	 */
	public boolean inTransaction() throws JDOException {
		return false;
	}

	public IndexDef createIndexDef(String indexName, String tableName, Iterable<String> columns, IndexDef.Type indexType, IndexDef.Using using) throws JDOException  {
		throw new JDOUserException("HBTableDataSource.gcreateIndexDef() HBase does not support secondary indexes");
	}

	/**
	 * Callable statements are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	public Object call(String statement, Param... parameters) throws JDOException {
		throw new JDOUnsupportedOptionException("HBase does not support callable statements");
	}

	protected void addTable(HBSchemalessTable oTbl) {
		oOTbls.add(oTbl);
	}
}
