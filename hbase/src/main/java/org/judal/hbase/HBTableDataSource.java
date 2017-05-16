package org.judal.hbase;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;

import com.knowgate.debug.DebugFile;
import com.knowgate.io.FileUtils;

import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.table.View;
import org.judal.storage.Param;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringBufferInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
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
public class HBTableDataSource implements TableDataSource {

	// A constant to convert a fraction to a percentage
	private static final int CONVERT_TO_PERCENTAGE = 100;

	private StringBufferInputStream oInStrm1, oInStrm2;

	private Configuration oCfg;

	private SchemaMetaData oSmd;

	private Set<HBTable> oOTbls;

	public HBTableDataSource(SchemaMetaData oMetaData) {
		oSmd = oMetaData;
		oOTbls = Collections.synchronizedSet(new HashSet<HBTable>());
		oInStrm1 = oInStrm2 = null;
	}

	public HBTableDataSource(String sPath, SchemaMetaData oMetaData) throws JDOException {
		oSmd = oMetaData;
		oOTbls = Collections.synchronizedSet(new HashSet<HBTable>());
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
		if (DebugFile.trace) DebugFile.writeln("this version is "+thisVersion);
		if (null!=thisVersion)
			if (!thisVersion.equals(defaultsVersion))
				throw new IOException(
						"hbase-default.xml file seems to be for and old version of HBase (" +
								defaultsVersion + "), this version is " + thisVersion);
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
			DebugFile.writeln("Begin HBConfig.open("+sPath+")");
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
			DebugFile.writeln("End HBConfig.open()");
		}
	}

	public void closeTables() throws JDOException {
		Iterator<HBTable> oIter = oOTbls.iterator();
		try {
			while (oIter.hasNext()) oIter.next().getTable().close();
		} catch (IOException ioe) {
			throw new JDOException(ioe.getMessage(), ioe);  
		}
		oOTbls.clear();	
	}

	@Override
	public void close() throws JDOException {
		closeTables();
		try {
			if (oInStrm1!=null) oInStrm1.close();
			if (oInStrm2!=null) oInStrm2.close();
		} catch (IOException ignore) { }
		oCfg = null;
	}

	@Override
	public SchemaMetaData getMetaData() throws JDOException {
		return oSmd;
	}

	@Override
	public void setMetaData(SchemaMetaData oSmd) {
		this.oSmd = oSmd;
	}

	@Override
	public void createTable(TableDef tableDef, Map<String,Object> options) throws JDOException {
		HTableDescriptor oTds;
		HBaseAdmin oAdm = null;
		String tableName = tableDef.getName();
		try {
			oAdm = new HBaseAdmin(getConfig());
			if (DebugFile.trace) DebugFile.writeln("HBaseAdmin.getTableDescriptor("+tableName+")");
			oAdm.getTableDescriptor(Bytes.toBytes(tableName));
			throw new JDOException("HBTableDataSource.createTable() Table "+tableName+" already exists");
		} catch (TableNotFoundException tnfe) {
			if (DebugFile.trace) DebugFile.writeln("Creating table "+tableName);
			oTds = new HTableDescriptor(tableName);
			for (ColumnDef oCol : tableDef.getColumns()) {
				String sFamily = oCol.getFamily();
				if (null==sFamily) 
					throw new JDOUserException("Family for column "+oCol.getName()+" cannot be null");
				if (null==sFamily) sFamily = "default";
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
			try {
				oAdm.createTable(oTds);
				oSmd.addTable(tableDef,null);
			} catch (IOException ioe) {
				throw new JDOException(ioe.getClass().getName()+" "+ioe.getMessage(), ioe);
			}
			if (DebugFile.trace) DebugFile.writeln("Table "+tableName+" created");
		} catch (IOException ioe) {
			throw new JDOException(ioe.getClass().getName()+" "+ioe.getMessage(), ioe);
	    } finally {
			try {
				if (oAdm!=null) oAdm.close();
			} catch (IOException e) { }
	    }
	}

	@Override
	public HBTable openTable(Record oRec) throws JDOException {
		try {
			if (DebugFile.trace) DebugFile.writeln("new HBTable(this, new HTable(getConfig(), "+oRec.getTableName()+"))");
			HBTable oTbl = new HBTable(this, new HTable(getConfig(), oRec.getTableName()), oRec.getClass());
			oOTbls.add(oTbl);
			return oTbl;
		} catch (TableNotFoundException tnf) {
			throw new JDOException("TableNotFoundException "+tnf.getMessage(), tnf);
		} catch (IOException ioe) {
			throw new JDOException("IOException "+ioe.getMessage(), ioe);
	  } catch (Exception xcpt) {
		  throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
		}
	}

	public Set<HBTable> openedTables()  {
		return oOTbls;
	}

	@Override
	public void dropTable(String sName, boolean bCascade) throws JDOException {
		if (bCascade)
			throw new JDOUnsupportedOptionException("HBase does not support drop cascade option");
		HBaseAdmin oAdm = null;
		try {
			final byte[] byName = Bytes.toBytes(sName);
			oAdm = new HBaseAdmin(getConfig());
			oAdm.disableTable(byName);
			oAdm.deleteTable(byName);
		} catch (MasterNotRunningException mnre) {
			throw new JDOException("HBTable.truncate() MasterNotRunningException "+mnre.getMessage(), mnre);		  
		} catch (ZooKeeperConnectionException zkce) {
			throw new JDOException("HBTable.truncate() ZooKeeperConnectionException "+zkce.getMessage(), zkce);		  		  
		} catch (IOException ioe) {
			throw new JDOException("HBTable.truncate() IOException "+ioe.getMessage(), ioe);		  
		} finally {
			try { if (oAdm!=null) oAdm.close(); } catch (Exception ignore) { }
		}
	}

	@Override
	public boolean exists(String objectName, String objectType) throws JDOException {
		boolean objExists;
		if (!objectType.equals("U"))
			throw new JDOUnsupportedOptionException("HBase only supports type U (table) exists check");
		HBaseAdmin oAdm = null;
		try {
			oAdm = new HBaseAdmin(getConfig());
			if (DebugFile.trace) DebugFile.writeln("HBaseAdmin.getTableDescriptor("+objectName+")");
			oAdm.getTableDescriptor(Bytes.toBytes(objectName));
			objExists = true;
		} catch (TableNotFoundException tnfe) {
			objExists = false;
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
		} finally {
			try {
			 if (oAdm!=null)
				 oAdm.close();
			 } catch (IOException e) { }
		}
		return objExists;
	}

	@Override
	public Map<String, String> getProperties() {
		return null;
	}

	/**
	 * Transactions are not supported by HBase. Therefore this method will always return <b>null</b>.
	 * @return <b>null</b>
	 */
	@Override
	public TransactionManager getTransactionManager() {
		return null;
	}

	/**
	 * @return <b>null</b>
	 */
	@Override
	public JDOConnection getJdoConnection() throws JDOException {
		return null;
	}

	/**
	 * Sequences are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public Sequence getSequence(String name) throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("HBase does not support sequences");
	}

	/**
	 * Transactions are not supported by HBase. Therefore this method will always return <b>false</b>.
	 * @return boolean <b>false</b>
	 */
	@Override
	public boolean inTransaction() throws JDOException {
		return false;
	}

	@Override
	public TableDef getTableDef(String tableName) throws JDOException {
		return getMetaData().getTable(tableName);
	}

	@Override
	public TableDef createTableDef(String tableName, Map<String, Object> options) throws JDOException {
		return new TableDef(tableName);
	}

	@Override
	public void truncateTable(String tableName, boolean cascade) throws JDOException {
		HBaseAdmin oAdm = null;
		TableDef tblDef = getTableDef(tableName);
		try {
			final byte[] byName = Bytes.toBytes(tableName);
			oAdm = new HBaseAdmin(getConfig());
			HTableDescriptor oTds = oAdm.getTableDescriptor(byName);
			oAdm.disableTable(byName);
			oAdm.deleteTable(byName);
			oAdm.createTable(oTds);
			createTable(tblDef, new HashMap<String,Object>());
		} catch (MasterNotRunningException mnre) {
			throw new JDOException("HBTable.truncate() MasterNotRunningException "+mnre.getMessage(), mnre);		  
		} catch (ZooKeeperConnectionException zkce) {
			throw new JDOException("HBTable.truncate() ZooKeeperConnectionException "+zkce.getMessage(), zkce);		  		  
		} catch (IOException ioe) {
			throw new JDOException("HBTable.truncate() IOException "+ioe.getMessage(), ioe);		  
		} finally {
			try { if (oAdm!=null) oAdm.close(); } catch (Exception ignore) { }
		}		
	}

	/**
	 * Secondary indexes are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public IndexableTable openIndexedTable(Record recordInstance) throws JDOUnsupportedOptionException {
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
		throw new JDOUnsupportedOptionException("HBase does not support tables with secondary indexes");
	}

	/**
	 * Callable statements are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public Object call(String statement, Param... parameters) throws JDOException {
		throw new JDOUnsupportedOptionException("HBase does not support callable statements");
	}

	/**
	 * Inner joins are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public IndexableView openInnerJoinView(Record recordInstance1, String joinedTableName, Entry<String, String> column)
			throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("HBase does not support inner join views");
	}

	/**
	 * Inner joins are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public IndexableView openInnerJoinView(Record recordInstance1, String joinedTableName,
			Entry<String, String>[] columns) throws JDOException {
		throw new JDOUnsupportedOptionException("HBase does not support inner join views");
	}

	/**
	 * Outer joins are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public IndexableView openOuterJoinView(Record recordInstance1, String joinedTableName, Entry<String, String> column)
			throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("HBase does not support outer join views");
	}

	/**
	 * Outer joins are not supported by HBase. Therefore this method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public IndexableView openOuterJoinView(Record recordInstance1, String joinedTableName,
			Entry<String, String>[] columns) throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("HBase does not support outer join views");
	}
	
}
