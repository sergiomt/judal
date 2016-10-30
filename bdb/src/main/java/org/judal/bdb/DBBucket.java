package org.judal.bdb;

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

import java.io.Serializable;
import java.sql.Types;
import java.util.HashSet;
import java.util.Iterator;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;
import javax.jdo.metadata.PrimaryKeyMetadata;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DeadlockException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;
import com.knowgate.debug.DebugFile;
import com.knowgate.debug.StackTraceUtil;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.PrimaryKeyDef;
import org.judal.serialization.BytesConverter;
import org.judal.storage.Bucket;
import org.judal.storage.Param;
import org.judal.storage.Stored;

public class DBBucket implements Bucket {

	private String sCnm;
	private String sDbk;
	private DBDataSource oRep;
	private Database oPdb;
	private Database oFdb;
	private StoredClassCatalog oCtg;
	private EntryBinding oKey;
	private Class<? extends Stored> oClass;
	private HashSet<DBIterator> oItr;
	
	@SuppressWarnings("unused")
	private DBBucket() { }

	// --------------------------------------------------------------------------

	protected DBBucket(DBDataSource oDatabaseEnvironment,
			String sConnectionName,
			String sDatabaseName,
			Database oPrimaryDatabase,
			Database oForeignDatabase,
			StoredClassCatalog oClassCatalog,
			EntryBinding oEntryBind) throws JDOException {

		sCnm = sConnectionName;
		oRep = oDatabaseEnvironment;
		sDbk = sDatabaseName;
		oPdb = oPrimaryDatabase;
		oFdb = oForeignDatabase;
		oCtg = oClassCatalog;
		oKey = oEntryBind;
		oItr = null;
		oClass = DBStored.class;
	}

	protected void finalize() {
		try { close(); } catch (JDOException ignore) {}
	}

	public StoredClassCatalog getCatalog() {
		return oCtg;
	}
	
	public Database getDatabase() {
		return oPdb;
	}

	public DBDataSource getDataSource() {
		return oRep;
	}

	protected Transaction getTransaction() throws JDOException {
		return oRep.getTransaction();
	}

	public boolean isReadOnly() throws JDOException {
		try {
			return oPdb.getConfig().getReadOnly();
		} catch (DatabaseException dbe) {
			throw new JDOException(dbe.getMessage(), dbe);
		}
	}
	
	@Override
	public String name() {
		return sCnm.substring(0, sCnm.indexOf('.'));
	}

	@Override
	public boolean exists(Object oKey)
		throws NullPointerException, IllegalArgumentException, JDOException {

		if (oKey==null) throw new NullPointerException("DBTable.exists() Param cannot be null");

		Object oVal;
		int iType;

		if (oKey instanceof Param) {
			oVal = ((Param) oKey).getValue();
			iType = ((Param) oKey).getType();
		} else {
			oVal = oKey;
			iType = ColumnDef.getSQLType(getPrimaryKey().getColumns()[0].getSQLType());
		}

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBBucket.exists("+oVal.getClass().getName()+" "+oVal+" at "+sCnm+"."+sDbk+")");
			DebugFile.incIdent();
			DebugFile.writeln("BytesConverter.toBytes("+oVal+", "+ColumnDef.typeName(iType)+")");
		}
		
		byte[] aByKey = BytesConverter.toBytes(oVal, iType);
		
		if (DebugFile.trace) {
			StringBuffer oStrKey = new StringBuffer(aByKey.length*3);
			for (int b=0; b<aByKey.length; b++) oStrKey.append(" "+Integer.toHexString(aByKey[b]));
			DebugFile.writeln("raw key hex is"+oStrKey.toString());
		}
		
		boolean bRetVal = false;
		OperationStatus oOpSt;

		try {
			oOpSt = oPdb.get(getTransaction(), new DatabaseEntry(aByKey), new DatabaseEntry(), LockMode.DEFAULT);
			bRetVal = (OperationStatus.SUCCESS==oOpSt);
		} catch (DeadlockException dlxc) {
			throw new JDOException(dlxc.getMessage(), dlxc);
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}

		if (DebugFile.trace) {
			if (oOpSt!=OperationStatus.SUCCESS) DebugFile.writeln("get() operation status was "+oOpSt.toString());
			DebugFile.decIdent();
			DebugFile.writeln("End DBBucket.exists() : "+String.valueOf(bRetVal));
		}

		return bRetVal;
	}

	@Override
	public boolean load(Object oKey, Stored oTarget) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBBucket.load("+oKey.getClass().getName()+" "+oKey+", Stored)");
			DebugFile.incIdent();
		}
		
		boolean retval = load(oKey, oTarget, BytesConverter.toBytes(oKey, ColumnDef.getSQLType(getPrimaryKey().getColumns()[0].getSQLType())));

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBBucket.load() : " + String.valueOf(retval));
		}
		return retval;
	}
	
	protected boolean load(Object oKey, Stored oTarget, byte[] byKey) throws JDOException {

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBBucket.load("+oKey.getClass().getName()+" "+oKey+", Stored, byte[])");
			DebugFile.incIdent();
			StringBuffer oStrKey = new StringBuffer(byKey.length*3);
			for (int b=0; b<byKey.length; b++) oStrKey.append(" "+Integer.toHexString(byKey[b]));
			DebugFile.writeln("raw key hex is"+oStrKey.toString());
		}

		DBEntityWrapper oDbEnt = null;

		try {
			DatabaseEntry oDbKey = new DatabaseEntry(byKey);
			DatabaseEntry oDbDat = new DatabaseEntry();
			if (OperationStatus.SUCCESS==oPdb.get(getTransaction(), oDbKey, oDbDat, LockMode.DEFAULT)) {
				DBEntityBinding oDbeb = new DBEntityBinding(oCtg);
				oDbEnt = oDbeb.entryToObject(oDbKey,oDbDat);
				oTarget.setKey(oDbEnt.getKey());
				oTarget.setValue(oDbEnt.getWrapped());
			}
		} catch (DeadlockException dlxc) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new JDOException(dlxc.getMessage(), dlxc);
		} catch (Exception xcpt) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new JDOException(xcpt.getMessage(), xcpt);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBBucket.load() : " + String.valueOf(oDbEnt!=null));
		}

		return oDbEnt!=null;
	} // load

	protected void closeIndexes() throws DatabaseException {
		// Overriden at DBTable
	}

	@Override
	public void close() throws JDOException {

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.close()");
			DebugFile.incIdent();
		}

		try {
			closeIndexes();
			oRep.ungetConnection(sCnm);
			if (oFdb!=null) oFdb.close();
			oFdb = null;
			if (oPdb!=null) oPdb.close();
			oPdb = null;
		} catch (DatabaseException dbe) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new JDOException(dbe.getMessage(), dbe);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.close()");
		}
	} // close

	@Override
	public void setClass(Class<Stored> candidateClass) {
		oClass = candidateClass;
	}

	@Override
	public void close(Iterator<Stored> oIter) {
		if (oItr.contains(oIter))
			oItr.remove(oIter);
		((DBIterator) oIter).close();
	}

	@Override
	public void closeAll() {
		for (DBIterator oIter : oItr)
			oIter.close();
		oItr.clear();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Class<Stored> getCandidateClass() {
		return (Class<Stored>) oClass;
	}

	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		return null;
	}

	public PrimaryKeyMetadata getPrimaryKey() {
		PrimaryKeyDef pk = new PrimaryKeyDef();
		ColumnDef cdef = (ColumnDef) pk.newColumnMetadata();
		cdef.setName("key");
		cdef.setType(Types.VARCHAR);
		cdef.setJDBCType("VARCHAR");
		cdef.setSQLType("VARCHAR");
		cdef.setLength(256);
		cdef.setAllowsNull(false);
		cdef.setPosition(1);
		return pk;
	}
		
	@Override
	public boolean hasSubclasses() {
		return false;
	}

	@Override
	public Iterator<Stored> iterator() {
		DBIterator oIter = new DBIterator(oPdb, oCtg);
		oItr.add(oIter);
		return oIter;
	}

	@Override
	public void store(Stored oRec) throws JDOException {
		store(oRec, BytesConverter.toBytes(oRec.getKey(), ColumnDef.getSQLType(getPrimaryKey().getColumns()[0].getSQLType())));
	}

	protected void store(Stored oRec, byte[] byKey) throws JDOException {
		
		if (isReadOnly()) throw new JDOException("DBBucket.store() table "+name()+" is in read-only mode");

		if (oRec==null) throw new NullPointerException("DBTable.store() Record to be stored is null");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBBucket.store("+name()+"."+oRec.getKey()+")");
			DebugFile.incIdent();
		}

		try {
		
			DBEntityWrapper oEwrp = new DBEntityWrapper(byKey, (Serializable) oRec.getValue());
			DBEntityBinding oDbeb = new DBEntityBinding(oCtg);

			if (DebugFile.trace) {
				StringBuffer oStrKey = new StringBuffer(byKey.length*3);
				for (int b=0; b<byKey.length; b++) oStrKey.append(" "+Integer.toHexString(byKey[b]));
				DebugFile.writeln("raw key hex is "+oStrKey.toString());
			}

			DatabaseEntry oDbKey = new DatabaseEntry(byKey);
			DatabaseEntry oDbDat = new DatabaseEntry(oDbeb.objectToData(oEwrp));

			if (DebugFile.trace) DebugFile.writeln("Database.put("+(getTransaction()==null ? "null" : getTransaction())+","+oRec.getKey()+",byte["+oDbDat.getSize()+"]="+oDbDat.getData()+")");
			
			oPdb.put(getTransaction(), oDbKey, oDbDat);
			
		} catch (DeadlockException dlxc) {
			if (DebugFile.trace) {
				DebugFile.writeln("DeadlockException "+dlxc.getMessage());
				try { DebugFile.writeln(StackTraceUtil.getStackTrace(dlxc)); } catch (java.io.IOException ignore) {}
				DebugFile.decIdent();
			}
			throw new JDOException(dlxc.getMessage(), dlxc);  	  
		} catch (DatabaseException xcpt) {
			if (DebugFile.trace) {
				DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage());
				try { DebugFile.writeln(StackTraceUtil.getStackTrace(xcpt)); } catch (java.io.IOException ignore) {}
				DebugFile.decIdent();
			}
			throw new JDOException(xcpt.getMessage(), xcpt);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBBucket.store() : "+oRec.getKey());
		}

	} // store
	
	
	public void delete(Object oKeyValue, Transaction oTrans) throws JDOException {
		try {
			oPdb.delete(oTrans, new DatabaseEntry(BytesConverter.toBytes(oKeyValue, ColumnDef.getSQLType(getPrimaryKey().getColumns()[0].getSQLType()))));
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
	} // delete
	
	@Override
	public void delete(Object oKey) throws JDOException {
		
		Object oPkv;
		
		if (oKey instanceof Param)
			oPkv = ((Param) oKey).getValue();
		else
			oPkv = oKey;

		delete (oPkv, getTransaction());
	} // delete

	public void truncate() throws JDOException {

		if (isReadOnly()) throw new JDOException("DBBucket.truncate() database "+name()+" is in read-only mode");

		try {
			oPdb.truncate(getTransaction(), false);
		} catch (DeadlockException dlxc) {
			throw new JDOException(dlxc.getMessage(), dlxc);
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		} 
	} // truncate
	
}
