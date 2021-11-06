package org.judal.bdbj;

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

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.judal.metadata.ViewDef;
import org.judal.storage.DataSource;
import org.judal.storage.Param;
import org.judal.storage.table.Record;
import org.judal.transaction.DataSourceTransaction;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;

import com.knowgate.debug.DebugFile;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.SecondaryMultiKeyCreator;
import com.sleepycat.je.Transaction;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;

public class DBJDataSource implements DataSource {

	// --------------------------------------------------------------------------

	private static final String CLASS_CATALOG = "JavaClassCatalog";

	// --------------------------------------------------------------------------

	private final boolean bTransactional;
	private boolean bReadOnly;
	private String sPath;
	private Environment oEnv;
	private EnvironmentConfig oCfg;
	private DatabaseConfig oDfg;
	private DatabaseConfig oDro;
	private StoredClassCatalog oCtg;
	private EntryBinding oKey;
	TransactionManager oTmn;
	private Random oRnd;

	private ConcurrentHashMap<String, DBJBucket> oConnectionMap;

	// --------------------------------------------------------------------------

	public DBJDataSource(Map<String,String> properties, TransactionManager transactManager, SchemaMetaData metaData) throws JDOException {
		oTmn = transactManager;
		bTransactional = (oTmn!=null);
		if (DebugFile.trace) DebugFile.writeln("open("+properties.get(DataSource.DBENV)+", null, null, false)");
		open (properties.get(DataSource.DBENV), null, null, false);
	}

	protected void finalize() {
		try { close(); } catch (JDOException ignore) {}
	}
	
	public SecondaryKeyCreator getKeyCreator(Class<? extends Record> oRecCls, TableDef oTbl, String sColumnName, int iColumnType) {
		return new DBSecondaryIndexCreator(new DBJEntityBinding(oCtg), oRecCls, oTbl, sColumnName, iColumnType);
	}

	public SecondaryMultiKeyCreator getMultiKeyCreator(Class<? extends Record> oRecCls, TableDef oTbl, String sIdx) {
		return new DBSecondaryMultiIndexCreator(new DBJEntityBinding(oCtg), oRecCls, oTbl, sIdx);
	}

	public Environment getEnvironment() {
		return oEnv;
	} // getEnvironment

	public boolean isTransactional() {
		return bTransactional;
	}

	public void open(String sDbEnv, String sUser, String sPassw, boolean bReadOnlyMode) throws JDOException,IllegalStateException,IllegalArgumentException {

		if (oEnv!=null)
			throw new IllegalStateException("Berkeley DB Environment is already open");
			
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBEnvironment.open("+sDbEnv+", "+sUser+", ..., "+String.valueOf(bReadOnlyMode)+")");
			DebugFile.incIdent();
		}

		try {
			if (null==sDbEnv) throw new IllegalArgumentException(DataSource.DBENV+" property may not be null");
			sPath = sDbEnv.endsWith(java.io.File.separator) ? sDbEnv : sDbEnv + java.io.File.separator;

			oRnd = new Random();

			oConnectionMap = new ConcurrentHashMap<String, DBJBucket>();

			bReadOnly = bReadOnlyMode;

			oCfg = new EnvironmentConfig();
			oCfg.setAllowCreate(true);

			if (bTransactional) {
				oCfg.setTransactional(true);
			}

			oCfg.setReadOnly(bReadOnly);

			oDfg = new DatabaseConfig();
			oDfg.setTransactional(bTransactional);
			oDfg.setSortedDuplicates(false);
			oDfg.setAllowCreate(true);
			oDfg.setReadOnly(false);

			oDro = new DatabaseConfig();
			oDro.setTransactional(bTransactional);
			oDro.setSortedDuplicates(false);
			oDro.setAllowCreate(true);
			oDro.setReadOnly(true);

			if (DebugFile.trace) DebugFile.writeln("Creating new Environment at "+sDbEnv);

			oEnv = new Environment(new File(sDbEnv), oCfg);	  

			DatabaseConfig oCtf = new DatabaseConfig();
			oCtf.setTransactional(bTransactional);
			oCtf.setAllowCreate(true);

			Database oJcc = oEnv.openDatabase(null, CLASS_CATALOG, bReadOnly ? oDro : oDfg);

			oCtg = new StoredClassCatalog(oJcc);
			try {
				oKey = new SerialBinding(oCtg, Class.forName("java.lang.String"));
			} catch (ClassNotFoundException neverthrown) { }

		} catch (DatabaseException dbe) {
			if (DebugFile.trace) {
				DebugFile.writeln("DatabaseException "+dbe.getMessage());
				DebugFile.decIdent();
			}
			throw new JDOException(dbe.getMessage(), dbe);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBEnvironment.open()");
		}

	} // DBEnvironment

	protected void joinTransaction() throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBDataSource.joinTransaction()");
			DebugFile.incIdent();
			DebugFile.writeln("is transactional = "+String.valueOf(isTransactional()));
			DebugFile.writeln("transaction manager = "+getTransactionManager());
		}
		try {
			if (isTransactional() && getTransactionManager()!=null) {
				DataSourceTransaction oTrn = (DataSourceTransaction) getTransactionManager().getTransaction();
				boolean bEnlisted = false;
				for (XAResource oRes : oTrn.listResources()) {
					if (oRes instanceof DBJTransactionalResource)
						if (getEnvironment().equals(((DBJTransactionalResource) oRes).environment())) {
							bEnlisted = true;
							break;
						}
				}
				if (!bEnlisted) {
					if (DebugFile.trace)
						DebugFile.writeln("Transaction.enlistResource(Environment)");
					getTransactionManager().getTransaction().enlistResource(new DBJTransactionalResource(getEnvironment()));
				}
			}
		} catch (IllegalStateException | RollbackException | SystemException xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBDataSource.joinTransaction()");
		}
	}

	protected synchronized DBJBucket openBucket(Properties oConnectionProperties, ViewDef viewDef, boolean useTransaction)
			throws JDOException,IllegalArgumentException,IllegalStateException {
		DBJBucket oDbc = null;
		Database oPdb = null;
		Database oFdb = null;
		String[] aIdxs = null;
		String sIdx = null;
		String sRel = null;
		String sFdb = null;
		String sDbk = oConnectionProperties.getProperty("name","unnamed");
		boolean bRo = false;
		String sCnm = "";
		String sDbp = "";
		HashMap<String,DBIndex> oIdxs = new HashMap<String,DBIndex>();

		if (null==oEnv)
			throw new IllegalStateException("Berkeley DB Environment has not been initialized or has been closed");
		
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBDataSource.openTableOrBucket("+oConnectionProperties + "," + viewDef.getName() + ",null,useTransaction=" + useTransaction + ")");
			DebugFile.incIdent();
		}

		try {

			// For Berkeley DB Java only
			String sRo = oConnectionProperties.getProperty("readonly");
			if (null!=sRo) bRo = (sRo.equalsIgnoreCase("true") || sRo.equalsIgnoreCase("1")) || oCfg.getReadOnly();

			if (isTransactional() && inTransaction() && useTransaction)
				joinTransaction();

			final Transaction oTrn = isTransactional() && inTransaction() && useTransaction ? getTransaction() : null;

				oPdb = oEnv.openDatabase(null, sDbk, bRo ? oDro : oDfg);

				if (DebugFile.trace) {
					DebugFile.writeln("Environment.openDatabase("+getTransaction()+","+getPath()+sDbk+".db"+","+sDbk+","+String.valueOf(bRo)+")");
					DebugFile.writeln("without secondary indexes");
				}
				sDbp = getPath()+sDbk+".db";

			sCnm = sDbk + ".PrimaryKey:" + String.valueOf(new java.util.Date().getTime()) + "#" + String.valueOf(oRnd.nextInt());

			oDbc = new DBJBucket(this, sCnm, sDbk, oPdb, oFdb, oCtg, oKey);

			oConnectionMap.put(sCnm, oDbc);

		} catch (DatabaseException dbe) {
			if (DebugFile.trace) {
				DebugFile.writeln("DBEnvironment.openTable() DatabaseException " + dbe.getMessage() + " "  + sDbp);
				DebugFile.decIdent();
			}
			throw new JDOException(dbe.getMessage(), dbe);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBDataSource.openTableOrBucket()");
		}

		return oDbc;
	} // openTable


	protected void ungetConnection(String sConnectionName) {
		if (oConnectionMap.containsKey(sConnectionName)) oConnectionMap.remove(sConnectionName);
	}

	public String getPath() {
		return sPath;
	}

	protected void closeTables()
			throws JDOException {

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBEnvironment.closeTables()");
			DebugFile.incIdent();
		}

		for (String s : oConnectionMap.keySet()) {
			oConnectionMap.get(s).close();
		}

		oConnectionMap.clear();

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBEnvironment.closeTables()");
		}

	} // closeTables

	// --------------------------------------------------------------------------

	@Override
	public void close() throws JDOException {

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBDataSource.close()");
			DebugFile.incIdent();
		}

		try {
			if (!isClosed()) {  	  

				if (inTransaction())
					getTransaction().abort();

				closeTables();

				if (oCtg!=null) {
					if (DebugFile.trace) DebugFile.writeln("Closing StoredClassCatalog");
					// Closing the StoredClassCatalog will close its underlying database as a side effect
					oCtg.close();
					oCtg = null;
				}

				if (DebugFile.trace) DebugFile.writeln("Closing Environment");
				oEnv.close();
				if (DebugFile.trace) DebugFile.writeln("Environment Closed");
				oEnv = null;
			}
		} catch (DatabaseException dbe) {
			if (DebugFile.trace) {
				DebugFile.writeln("DatabaseException "+dbe.getMessage());
				DebugFile.decIdent();
			}
			throw new JDOException(dbe.getMessage(),dbe);
		} catch (Exception xcpt) {
			if (DebugFile.trace) {
				DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage());
				DebugFile.decIdent();
			}
			throw new JDOException(xcpt.getMessage(),xcpt);
		}
		
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBDataSource.close()");
		}
	} // close

	public boolean isClosed() {
		return oEnv==null;
	}

	public boolean isReadOnly() {
		return bReadOnly;
	}

	@Override
	public boolean exists(String objectName, String objectType) throws JDOException {
		if (objectType.equals("U"))
			return new File(getPath() + objectName + ".db").exists();
		else
			return false;
	}

	@Override
	public Map<String, String> getProperties() {
		return new HashMap<>();
	}

	@Override
	public TransactionManager getTransactionManager() {
		return oTmn;
	}

	public Transaction getTransaction() throws JDOException {
		if (DebugFile.trace) {
			try {
			if (null==oTmn)
				DebugFile.writeln("DBDataSource.getTransaction() no transaction manager present");
			else
				DebugFile.writeln("DBDataSource.getTransaction() transaction status is "+((DataSourceTransaction) oTmn.getTransaction()).getStatusAsString());
			} catch (SystemException sx) {
				DebugFile.writeln("SystemException at DBDataSource.getTransaction() "+sx.getMessage());
			}
		}
		if (null==oTmn) return null;
		try {
			if (oTmn.getStatus()==Status.STATUS_NO_TRANSACTION) {
				return null;
			} else {
				List<XAResource> res = ((DataSourceTransaction) oTmn.getTransaction()).listResources();
				if (res.size()==0)
					throw new JDOException("Transaction has no enlisted resources");
				return ((DBJTransactionalResource) res.get(0)).transaction();
			}
		} catch (SystemException sx) {
			throw new JDOException(sx.getMessage(), sx);
		}
	}

	@Override
	public boolean inTransaction() throws JDOException {
		try {
			if (getTransactionManager()==null)
				return false;
			else if (getTransactionManager().getTransaction()==null)
				return false;
			else {
				final int status = getTransactionManager().getTransaction().getStatus();				
				if (DebugFile.trace)
					DebugFile.writeln("DBDataSource.inTransaction() transaction status is " + status);
				if (status==Status.STATUS_UNKNOWN)
					throw new JDOException("Transaction status is unknown");
				return status!=Status.STATUS_NO_TRANSACTION && status!=Status.STATUS_COMMITTED && status!=Status.STATUS_ROLLEDBACK;
			}
		} catch (SystemException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	@Override
	public JDOConnection getJdoConnection() throws JDOException {
		throw new JDOUnsupportedOptionException("Berkley DB does not use connections");
	}

	@Override
	public Sequence getSequence(String name) throws JDOException {
		return new DBJSequenceGenerator(this,name);
	}

	@Override
	public Object call(String statement, Param... parameters) throws JDOException {
		throw new JDOUnsupportedOptionException("Berkley DB does not support callable statements");
	}

}

