package org.judal.bdb;

import java.io.File;

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

import java.io.FileNotFoundException;

import java.sql.Types;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.JDOUserException;
import javax.jdo.metadata.PrimaryKeyMetadata;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.IndexDef.Using;
import org.judal.metadata.IndexDef.Type;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.serialization.Bytes;
import org.judal.serialization.BytesConverter;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.Param;
import org.judal.storage.StandardConstraintsChecker;
import org.judal.storage.StorageObjectFactory;
import org.judal.storage.keyvalue.ReadOnlyBucket;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.query.Operator;
import org.judal.storage.table.ArrayListRecordSet;
import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

import com.knowgate.debug.DebugFile;
import com.knowgate.debug.StackTraceUtil;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.JoinCursor;
import com.sleepycat.db.JoinConfig;
import com.sleepycat.db.Database;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.Transaction;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.SecondaryConfig;
import com.sleepycat.db.SecondaryCursor;
import com.sleepycat.db.SecondaryDatabase;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DeadlockException;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;

/**
 * Implementation of org.judal.storage.Table interface for Berkeley DB
 * @author Sergio Montoro Ten
 *
 */
public class DBTable extends DBBucket implements IndexableTable {

	private TableDef oTbl;
	private HashMap<String, DBIndex> oInd;
	private ConstraintsChecker oChk;
	private Class<? extends Record> cRecCls;

	// --------------------------------------------------------------------------

	protected DBTable(DBDataSource oDatabaseEnvironment, TableDef oTableDef, String sConnectionName,
			String sDatabaseName, Database oPrimaryDatabase, HashMap<String, DBIndex> oIndexes,
			Database oForeignDatabase, StoredClassCatalog oClassCatalog, EntryBinding oEntryBind,
			Class<? extends Record> cRecordClass, boolean useTransaction) throws JDOException {

		super(oDatabaseEnvironment, sConnectionName, sDatabaseName, oPrimaryDatabase, oForeignDatabase, oClassCatalog,
				oEntryBind);

		if (DebugFile.trace)
			DebugFile.writeln("new DBTable() "+ sConnectionName+" at "+sDatabaseName);
		
		oTbl = oTableDef;
		oInd = oIndexes;
		cRecCls = cRecordClass;
		setClass(cRecordClass);

		if (!isReadOnly() && oInd != null && oInd.keySet() != null) {
			for (String sColumnName : oInd.keySet()) {
				openIndex(sColumnName, useTransaction);
			} // next
		} // fi
	}

	// --------------------------------------------------------------------------

	@Override
	public Class<? extends Record> getResultClass() {
		return cRecCls;
	}
	
	// --------------------------------------------------------------------------

	public DBDataSource getDataSource() {
		return (DBDataSource) super.getDataSource();
	}

	// --------------------------------------------------------------------------

	protected void closeIndexes() throws DatabaseException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.closeIndexes()");
			DebugFile.incIdent();
		}
		if (oInd != null && oInd.keySet() != null) {
			for (String sColumnName : oInd.keySet()) {
				if (DebugFile.trace)
					DebugFile.writeln("closing index on "+sColumnName);
				oInd.get(sColumnName).close();
			}
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.closeIndexes()");
		}
	}

	// --------------------------------------------------------------------------

	@Override
	public ColumnDef[] columns() {
		ColumnDef[] oLst;
		try {
			String tableName = name();
			if (getDataSource() instanceof DBTableDataSource) {
				SchemaMetaData metaData = ((DBTableDataSource) getDataSource()).getMetaData();
				if (null == metaData) {
					if (DebugFile.trace)
						DebugFile.writeln("no metadata found for table" + tableName);
					return new ColumnDef[0];
				}
				ColumnDef[] oCols = metaData.getColumns(tableName);
				if (oCols != null)
					oLst = metaData.getColumns(name());
				else
					oLst = new ColumnDef[0];
			} else if (getDataSource() instanceof DBBucketDataSource) {
				oLst = ((DBBucketDataSource) getDataSource()).getKeyValueDef(name()).getColumns();
			} else {
				oLst = new ColumnDef[0];
			}
		} catch (Exception xcpt) {
			if (DebugFile.trace)
				DebugFile.writeln("DBTable.columns() " + xcpt.getClass().getName() + " " + xcpt.getMessage());
			oLst = null;
		}
		return oLst;
	}

	// --------------------------------------------------------------------------

	public DBIndex[] indexes() {
		DBIndex[] retval = new DBIndex[oInd.size()];
		int n = 0;
		for (Map.Entry<String,DBIndex> e :  oInd.entrySet())
			retval[n++] = e.getValue();
		return retval;
	}
	
	// --------------------------------------------------------------------------

	/*
	public ColumnDef getColumn(String sColumnName) {
		ColumnDef[] oCols = columns();
		if (oCols != null)
			for (ColumnDef oCol : oCols)
				if (oCol.getName().equalsIgnoreCase(sColumnName))
					return oCol;
		return null;
	}
	*/

	// --------------------------------------------------------------------------

	@Override
	public boolean exists(Param... params) throws JDOException {
		boolean bExists = false;

		if (null == params)
			throw new JDOException("DBTable.exists() Params may not be null");
		
		if (DebugFile.trace) {
			StringBuffer paramStr = new StringBuffer();
			for (Param p : params)
				paramStr.append((paramStr.length()==0 ? "" : ", ")+p.getName()+"="+p.getValue());
			DebugFile.writeln("Begin DBTable.exists("+paramStr.toString()+")");
			DebugFile.incIdent();
		}

		final int nValues = params.length;
		DatabaseEntry oDbDat = new DatabaseEntry();
		DatabaseEntry oDbKey = new DatabaseEntry();
		
		if (1 == nValues) {

			SecondaryCursor oCur = null;
			try {
				final String sColName = params[0].getName();
				final boolean usingPk = sColName.equalsIgnoreCase(getPrimaryKey().getColumn());

				if (DebugFile.trace)
					DebugFile.writeln(usingPk ? "using primary key" : "using secondary key");
				
				if (!usingPk && !oInd.containsKey(sColName))
					throw new JDOException("DBTable.exists() Column " + sColName + " is not a primary key nor a secondary index");
			
				if (DebugFile.trace)
					DebugFile.writeln("new DatabaseEntry("+params[0].getValue().getClass().getName()+" "+params[0].getValue()+")");

				oDbKey = new DatabaseEntry(BytesConverter.toBytes(params[0].getValue(), params[0].getType()));

				if (DebugFile.trace) {
					DebugFile.writeln("BytesConverter.toBytes("+params[0].getValue()+", "+ColumnDef.typeName(params[0].getType())+")");
					byte[] aByKey = BytesConverter.toBytes(params[0].getValue(), params[0].getType());
					StringBuffer oStrKey = new StringBuffer(aByKey.length*3);
					for (int b=0; b<aByKey.length; b++) oStrKey.append(" "+Integer.toHexString(aByKey[b]));
					DebugFile.writeln("raw key hex is"+oStrKey.toString());
				}

				if (usingPk) {
					if (DebugFile.trace)
						DebugFile.writeln("getDatabase().get(getTransaction(), DatabaseEntry, DatabaseEntry, LockMode.DEFAULT)");
					bExists = (OperationStatus.SUCCESS == getDatabase().get(getTransaction(), oDbKey, oDbDat, LockMode.DEFAULT));
				} else {
					DBIndex oIdx;
					oIdx = oInd.get(sColName);
					if (oIdx.isClosed())
						openIndex(sColName, getDataSource().isTransactional());
					oCur = oIdx.getCursor(getTransaction());
					if (DebugFile.trace)
						DebugFile.writeln("SecondaryCursor.getSearchKey(DatabaseEntry, DatabaseEntry, LockMode.DEFAULT)");
					bExists = (OperationStatus.SUCCESS == oCur.getSearchKey(oDbKey, oDbDat, LockMode.DEFAULT));
				}

			} catch (DatabaseException xcpt) {
				throw new JDOException(xcpt.getMessage());
			} finally {
				if (oCur != null) {
					try { oCur.close(); } catch (DatabaseException ignore) { }
					oCur = null;
				}				
			}

		} else {

			String sValue;

			JoinCursor oJur = null;

			OperationStatus[] aOst = new OperationStatus[nValues];
			DBIndex[] aIdxs = new DBIndex[nValues];
			SecondaryCursor[] aCurs = new SecondaryCursor[params.length];

			try {

				for (int sc = 0; sc < nValues; sc++) {
					if (params[sc].getValue() instanceof String)
						sValue = (String) params[sc].getValue();
					else
						sValue = params[sc].getValue().toString();
					aIdxs[sc] = oInd.get(params[sc].getName());
					if (aIdxs[sc].isClosed())
						openIndex(params[sc].getName(), getDataSource().isTransactional());
					aCurs[sc] = aIdxs[sc].getCursor(getTransaction());
					aOst[sc] = aCurs[sc].getSearchKey(new DatabaseEntry(BytesConverter.toBytes(sValue, Types.VARCHAR)), new DatabaseEntry(), LockMode.DEFAULT);
				} // next

				oJur = getDatabase().join(aCurs, JoinConfig.DEFAULT);

				bExists = (oJur.getNext(oDbKey, oDbDat, LockMode.DEFAULT) == OperationStatus.SUCCESS);

				oJur.close();
				oJur = null;

				for (int sc = nValues - 1; sc >= 0; sc--) {
					aCurs[sc].close();
					aIdxs[sc].close();
				}

			} catch (DeadlockException dlxc) {
				if (DebugFile.trace) {
					DebugFile.writeln(dlxc.getClass().getName() + " " + dlxc.getMessage());
					try {
						DebugFile.writeln(StackTraceUtil.getStackTrace(dlxc));
					} catch (Exception ignore) {
					}
					DebugFile.decIdent();
				}
				throw new JDOException(dlxc.getMessage(), dlxc);
			} catch (Exception xcpt) {
				if (DebugFile.trace) {
					DebugFile.writeln(xcpt.getClass().getName() + " " + xcpt.getMessage());
					try {
						DebugFile.writeln(StackTraceUtil.getStackTrace(xcpt));
					} catch (Exception ignore) {
					}
					DebugFile.decIdent();
				}
				throw new JDOException(xcpt.getMessage(), xcpt);
			} finally {
				try {
					if (oJur != null)
						oJur.close();
				} catch (Exception ignore) {
				}
				for (int sc = nValues - 1; sc >= 0; sc--) {
					try {
						if (aCurs[sc] != null)
							aCurs[sc].close();
					} catch (Exception ignore) {
					}
					try {
						if (aIdxs[sc] != null)
							aIdxs[sc].close();
					} catch (Exception ignore) {
					}
				} // next
			}
		} // fi

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.exists() : "+String.valueOf(bExists));
		}
		
		return bExists;
	}

	// --------------------------------------------------------------------------

	@Override
	public boolean load(Object oKey, Stored oTarget) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.load()");
			DebugFile.incIdent();
		}
		
		boolean retval = load(oKey, oTarget, BytesConverter.toBytes(oKey, oTbl.getColumnByName(oTbl.getPrimaryKeyMetadata().getColumn()).getType()));

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.load() : " + String.valueOf(retval));
		}
		return retval;
	}
	
	// --------------------------------------------------------------------------

	@Override
	public void delete(Object oKey) throws JDOException {

		byte[] byIndexValue;
		String sIndexName = null;
		Type eType;
		if (oKey instanceof Param) {
			Param oPar = ((Param) oKey);
			byIndexValue = BytesConverter.toBytes(oPar.getValue(), Types.VARCHAR);
			sIndexName = oPar.getName();
			eType = oPar.getIndexType();
		} else {
			byIndexValue = BytesConverter.toBytes(oKey, Types.VARCHAR);
			sIndexName = getPrimaryKey().getColumn();
			if (null == sIndexName)
				throw new JDOException("Cannot find primary key for table " + name());
			eType = Type.ONE_TO_ONE;
		}

		int iDeleted = 0;
		SecondaryDatabase oSdb = null;
		SecondaryCursor oCur = null;
		SecondaryConfig oSec = new SecondaryConfig();
		oSec.setAllowCreate(false);
		oSec.setAllowPopulate(false);
		oSec.setType(DatabaseType.BTREE);
		oSec.setReadOnly(true);

		try {
			switch (eType) {
			case ONE_TO_ONE:
			case MANY_TO_ONE:
				oSec.setKeyCreator(getDataSource().getKeyCreator(getResultClass(), oTbl, sIndexName,
						          ((ColumnDef) getPrimaryKey().getColumns()[0]).getType()));
				break;
			case ONE_TO_MANY:
			case MANY_TO_MANY:
				oSec.setMultiKeyCreator(getDataSource().getMultiKeyCreator(getResultClass(), oTbl, sIndexName));
				break;
			default:
				throw new JDOException("Unrecognized index type " + eType);
			}

			String sSecIdxPath = getDataSource().getPath() + getDatabase().getDatabaseName() + "." + sIndexName + ".db";

			oSdb = getDataSource().getEnvironment().openSecondaryDatabase(getTransaction(), sSecIdxPath,
					getDatabase().getDatabaseName() + "_" + sIndexName, getDatabase(), oSec);

			DBEntityBinding oDbeb = new DBEntityBinding(getCatalog());
			DatabaseEntry oDbKey = new DatabaseEntry(byIndexValue);
			DatabaseEntry oDbDat = new DatabaseEntry();
			oCur = oSdb.openSecondaryCursor(getTransaction(), null);

			ArrayList<byte[]> aKeys = new ArrayList<byte[]>(1000);

			OperationStatus oOst = oCur.getSearchKey(oDbKey, oDbDat, LockMode.DEFAULT);
			while (oOst == OperationStatus.SUCCESS) {
				iDeleted++;
				aKeys.add(oDbeb.objectToKey(oDbeb.entryToObject(oDbKey, oDbDat)));
				oOst = oCur.getNextDup(oDbKey, oDbDat, LockMode.DEFAULT);
			} // wend

			oCur.close();
			oCur = null;
			oSdb.close();
			oSdb = null;

			for (byte[] k : aKeys) {
				delete(k, getTransaction());
			}

		} catch (DeadlockException dlxc) {
			throw new JDOException(dlxc.getMessage(), dlxc);
		} catch (Exception xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		} finally {
			try {
				if (oCur != null)
					oCur.close();
			} catch (Exception ignore) {
			}
			try {
				if (oSdb != null)
					oSdb.close();
			} catch (Exception ignore) {
			}
		}
	} // delete

	// --------------------------------------------------------------------------

	@Override
	public int delete(Param[] where) throws JDOException {
		return (int) fetchOrDelete(null, Integer.MAX_VALUE, 0, true, where);
	}

	// --------------------------------------------------------------------------

	@Override
	public void insert(Param... aParams) throws JDOException {
		String sPkValue = null;
		TableDef tableDef = new TableDef(name());
		for (Param oPar : aParams) {
			if (oPar.isPrimaryKey())
				sPkValue = (String) oPar.getValue();
			tableDef.addColumnMetadata(oPar.getFamily(), oPar.getName(), oPar.getType(), oPar.getLength(), oPar.getScale(),
					true, null, null, null, oPar.isPrimaryKey());
		}
		if (null == sPkValue)
			throw new JDOException("DBTable.insert() cannot find primary key among parameters");

		DBStored oRec = new DBStored(tableDef);
		store(oRec);
	}

	// --------------------------------------------------------------------------

	private <R extends Record> R makeRecord(Class<R> recordClass, DBEntityBinding oDbeb, DatabaseEntry oDbKey, DatabaseEntry oDbDat) throws NoSuchMethodException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.makeRecord("+recordClass.getClass().getName()+")");
			DebugFile.incIdent();
		}
		R oRec = null;
		oRec = StorageObjectFactory.newRecord(recordClass, oTbl);
		DBEntityWrapper oWrp = oDbeb.entryToObject(oDbKey, oDbDat);
		oRec.setKey(oWrp.getKey());
		oRec.setValue(oWrp.getWrapped());
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.makeRecord() : "+oRec);
		}
		return oRec;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object indexValue) throws JDOException {
		return fetch (fetchGroup, indexColumnName, indexValue, ReadOnlyBucket.MAX_ROWS, 0);
	}
	
	// --------------------------------------------------------------------------
	
	@Override
	@SuppressWarnings("unchecked")
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object indexValue, int maxRows, int offset) throws JDOException {

		if (null == indexColumnName)
			throw new JDOException("DBTable.fetch() Column name may not be null");

		final boolean usingPk = indexColumnName.equalsIgnoreCase(getPrimaryKey().getColumn());
		
		if (!usingPk && !oInd.containsKey(indexColumnName))
			throw new JDOException(
					"DBTable.fetch() Column " + indexColumnName + " is not a primary key nor a secondary index");
				
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.fetch(" + indexColumnName + "," + indexValue + "," + String.valueOf(maxRows) + "," + String.valueOf(offset) + ")");
			DebugFile.incIdent();
			DebugFile.writeln(usingPk ? "using primary key" : "using secondary index");
		}

		String sIndexValue;

		if (null == indexValue)
			sIndexValue = "";
		else if (indexValue instanceof String)
			sIndexValue = (String) indexValue;
		else
			sIndexValue = indexValue.toString();

		if (maxRows <= 0)
			throw new JDOException("Invalid value for max rows parameter " + String.valueOf(maxRows));

		if (DebugFile.trace)
			DebugFile.writeln("new ArrayListRecordSet<R>(" + getResultClass().getName() + ")");

		ArrayListRecordSet<? extends Record> oEst = new ArrayListRecordSet<>(getResultClass());
		Cursor oPur = null;
		SecondaryCursor oCur = null;
		OperationStatus oOst;
		int fetched = 0;
		int skipped = 0;

		try {

			if (DebugFile.trace)
				DebugFile.writeln("new DBEntityBinding(getCatalog())");

			DBEntityBinding oDbeb = new DBEntityBinding(getCatalog());
			DatabaseEntry oDbDat = new DatabaseEntry();
			DatabaseEntry oDbKey = new DatabaseEntry();

			if (sIndexValue.equals("%") || sIndexValue.equalsIgnoreCase(Operator.ISNOTNULL)) {

				if (DebugFile.trace)
					DebugFile.writeln("Database.openCursor(getTransaction(),null)");

				oPur = getDatabase().openCursor(getTransaction(), null);

				oOst = oPur.getFirst(oDbKey, oDbDat, LockMode.DEFAULT);
				while (oOst == OperationStatus.SUCCESS && fetched<maxRows) {
					if (fetched+skipped>=offset) {
						Record oRec = (R) makeRecord(getResultClass(), oDbeb, oDbKey, oDbDat);
						if (!oRec.isNull(indexColumnName)) {
							oEst.add(oRec);
							fetched++;
						}
					} else {
						skipped++;
					}
					oOst = oPur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);						
				} // wend
				oPur.close();
				oPur = null;

			} else if (sIndexValue.equalsIgnoreCase("NULL") || sIndexValue.equalsIgnoreCase(Operator.ISNULL)) {

				if (DebugFile.trace)
					DebugFile.writeln("Database.openCursor(getTransaction(),null)");

				oPur = getDatabase().openCursor(getTransaction(), null);
				oOst = oPur.getFirst(oDbKey, oDbDat, LockMode.DEFAULT);
				while (oOst == OperationStatus.SUCCESS && fetched<maxRows) {
					if (fetched+skipped>=offset) {
						Record oRec = makeRecord(getResultClass(), oDbeb, oDbKey, oDbDat);
						if (oRec.isNull(indexColumnName)) {
							oEst.add((R) oRec);
							fetched++;
						}
					} else {
						skipped++;
					}
					oOst = oPur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);
				} // wend
				oPur.close();
				oPur = null;

			} else {

				if (usingPk) {
					if (DebugFile.trace)
						DebugFile.writeln("Database.openCursor(getTransaction(),null)");
					oPur = getDatabase().openCursor(getTransaction(), null);
				} else {
					DBIndex oIdx;
					oIdx = oInd.get(indexColumnName);
					if (oIdx.isClosed())
						openIndex(indexColumnName, getDataSource().isTransactional());
					oCur = oIdx.getCursor(getTransaction());
				}

				if (sIndexValue.endsWith("%")) {
					sIndexValue = sIndexValue.substring(0, sIndexValue.length() - 1);
					oDbKey = new DatabaseEntry(BytesConverter.toBytes(sIndexValue, oTbl.getColumnByName(indexColumnName).getType()));
					if (usingPk)
						oOst = oPur.getSearchKeyRange(oDbKey, oDbDat, LockMode.DEFAULT);
					else
						oOst = oCur.getSearchKeyRange(oDbKey, oDbDat, LockMode.DEFAULT);

					while (oOst == OperationStatus.SUCCESS && fetched<maxRows) {
						Record oRec = makeRecord(getResultClass(), oDbeb, oDbKey, oDbDat);
						try {
							oRec.getColumn(indexColumnName);
							if (oRec.getString(indexColumnName, "").startsWith(sIndexValue)) {
								if (fetched+skipped>=offset) {
									oEst.add((R) oRec);
									fetched++;
								} else {
									skipped++;
								}
								if (usingPk)
									oOst = oPur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);
								else
									oOst = oCur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);
							} else {
								oOst = OperationStatus.NOTFOUND;
							}
						} catch (ArrayIndexOutOfBoundsException columnnotfound) {
							oOst = OperationStatus.KEYEMPTY;
						}
					} // wend matches

				} else {

					if (DebugFile.trace)
						DebugFile.writeln("new DatabaseEntry(BytesConverter.toBytes(" + sIndexValue + ", " + ColumnDef.typeName(getColumnByName(indexColumnName).getType()) + "))");
					byte[] byKey = BytesConverter.toBytes(sIndexValue, getColumnByName(indexColumnName).getType());
					if (DebugFile.trace) {
						StringBuffer oStrKey = new StringBuffer(byKey.length*3);
						for (int b=0; b<byKey.length; b++) oStrKey.append(" "+Integer.toHexString(byKey[b]));
						DebugFile.writeln("raw key hex is"+oStrKey.toString());
					}
					oDbKey = new DatabaseEntry(byKey);
					if (usingPk) {
						if (DebugFile.trace)
							DebugFile.writeln("Database.get(" + sIndexValue + ", DatabaseEntry, LockMode.DEFAULT)");
						if (offset==0)
							oOst = getDatabase().get(getTransaction(), oDbKey, oDbDat, LockMode.DEFAULT);
						else
							oOst = OperationStatus.NOTFOUND;
						if (DebugFile.trace)
							DebugFile.writeln("get return status was "+oOst);
						if (oOst == OperationStatus.SUCCESS) {
							Record oRec = makeRecord(getResultClass(), oDbeb, oDbKey, oDbDat);
							if (DebugFile.trace) DebugFile.writeln("made record");
							oEst.add(oRec);
							fetched++;
							if (DebugFile.trace) DebugFile.writeln("fetched row "+String.valueOf(fetched)+" using pk");
						}
					} else {
						if (DebugFile.trace)
							DebugFile.writeln("SecondaryCursor.getSearchKey(" + sIndexValue + ", DatabaseEntry, LockMode.DEFAULT)");
						oOst = oCur.getSearchKey(oDbKey, oDbDat, LockMode.DEFAULT);
						if (DebugFile.trace)
							DebugFile.writeln("getSearchKey return status was "+oOst);
						while (oOst == OperationStatus.SUCCESS && fetched<maxRows) {
							if (fetched+skipped>=offset) {
								oEst.add(makeRecord(getResultClass(), oDbeb, oDbKey, oDbDat));
								fetched++;
								if (DebugFile.trace) DebugFile.writeln("fetched "+String.valueOf(fetched));
							} else {
								skipped++;
								if (DebugFile.trace) DebugFile.writeln("skipped "+String.valueOf(skipped));
							}
							oOst = oCur.getNextDup(oDbKey, oDbDat, LockMode.DEFAULT);
							if (DebugFile.trace)
								DebugFile.writeln("getNextDup return status was "+oOst);
						} // wend
					}

				} // fi

				if (oPur != null) {
					if (DebugFile.trace)
						DebugFile.writeln("closing primary index");
					oPur.close();
					oPur = null;
				}
				if (oCur != null) {
					if (DebugFile.trace)
						DebugFile.writeln("closing cursor index");
					oCur.close();
					oCur = null;
				}
			} // fi

		} catch (DeadlockException dlxc) {
			if (DebugFile.trace) {
				DebugFile.writeln(dlxc.getClass().getName() + " " + dlxc.getMessage());
				try {
					DebugFile.writeln(StackTraceUtil.getStackTrace(dlxc));
				} catch (Exception ignore) {
				}
				DebugFile.decIdent();
			}
			throw new JDOException(dlxc.getMessage(), dlxc);
		} catch (Exception xcpt) {
			if (DebugFile.trace) {
				DebugFile.writeln(xcpt.getClass().getName() + " " + xcpt.getMessage());
				try {
					DebugFile.writeln(StackTraceUtil.getStackTrace(xcpt));
				} catch (Exception ignore) {
				}
				DebugFile.decIdent();
			}
			throw new JDOException(xcpt.getMessage(), xcpt);
		} finally {
			try {
				if (oPur != null) {
					if (DebugFile.trace)
						DebugFile.writeln("finally closing primary index");
					oPur.close();
				}
			} catch (Exception ignore) {
			}
			try {
				if (oCur != null) {
					if (DebugFile.trace)
						DebugFile.writeln("finally closing cursor index");
					oCur.close();
				}
			} catch (Exception ignore) {
			}
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.fetch() : " + String.valueOf(oEst.size()));
		}

		return (RecordSet<R>) oEst;
	} // fetch

	public <R extends Record> RecordSet<R> fetch(final String sIndexColumn, String sIndexValueMin, String sIndexValueMax) throws JDOException {
		if (DebugFile.trace) {
			DebugFile
					.writeln("Begin DBTable.fetch(" + sIndexColumn + "," + sIndexValueMin + "," + sIndexValueMax + ")");
			DebugFile.incIdent();
		}

		if (null == sIndexValueMin)
			throw new JDOException("DBTable.fetch() Index minimum value may not be null");
		if (null == sIndexValueMax)
			throw new JDOException("DBTable.fetch() Index maximum value may not be null");

		final int iType = getColumnByName(sIndexColumn).getType();

		RecordSet<R> oEst = fetch(sIndexColumn, BytesConverter.toBytes(sIndexValueMin, iType),
				BytesConverter.toBytes(sIndexValueMax, iType));

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.fetch() " + String.valueOf(oEst.size()));
		}

		return oEst;
	}

	// --------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public <R extends Record> RecordSet<R> fetch(final String sIndexColumn, byte[] byMin, byte[] byMax) throws JDOException {

		if (null == sIndexColumn)
			throw new JDOException("DBTable.fetch() Column name may not be null");

		if (!oInd.containsKey(sIndexColumn))
			throw new JDOException("DBTable.fetch() Column " + sIndexColumn + " is not indexed");

		ArrayListRecordSet<? extends Record> oEst = new ArrayListRecordSet<>(getResultClass());
		Cursor oPur = null;
		SecondaryCursor oCur = null;
		OperationStatus oOst;

		try {

			Object oValue;
			DBEntityWrapper oDbEnt;
			DBEntityBinding oDbeb = new DBEntityBinding(getCatalog());
			DatabaseEntry oDbDat = new DatabaseEntry();
			DatabaseEntry oDbKey = new DatabaseEntry();
			;
			boolean bMinExists;

			DBIndex oIdx = oInd.get(sIndexColumn);
			if (oIdx.isClosed())
				openIndex(sIndexColumn, getDataSource().isTransactional());

			oCur = oIdx.getCursor(getTransaction());

			if (DebugFile.trace)
				DebugFile.writeln("got SecondaryCursor for " + sIndexColumn);

			oDbKey = new DatabaseEntry(byMin);
			oOst = oCur.getSearchKey(oDbKey, oDbDat, LockMode.DEFAULT);
			bMinExists = (oOst == OperationStatus.SUCCESS);
			oCur.close();
			oCur = null;

			if (DebugFile.trace)
				DebugFile.writeln(sIndexColumn + " has" + (bMinExists ? "" : " not") + " a minimum value");

			final int iType = getColumnByName(sIndexColumn).getType();

			if (bMinExists) {

				oCur = oIdx.getCursor(getTransaction());
				oDbKey = new DatabaseEntry(byMin);
				oOst = oCur.getSearchKey(oDbKey, oDbDat, LockMode.DEFAULT);
				while (oOst == OperationStatus.SUCCESS) {
					Record oRec = StorageObjectFactory.newRecord(getResultClass(), oTbl);
					oDbEnt = oDbeb.entryToObject(oDbKey, oDbDat);
					oRec.setKey(oDbEnt.getKey());
					oRec.setValue(oDbEnt.getWrapped());
					oValue = oRec.apply(sIndexColumn);
					if (oValue != null) {
						if (Bytes.compareTo(BytesConverter.toBytes(oValue, iType), byMax) > 0)
							break;						
						oEst.add((R) oRec);
					}
					oOst = oCur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);
				} // wend
				oCur.close();
				oCur = null;

			} else {

				oPur = getDatabase().openCursor(getTransaction(), null);
				oOst = oPur.getFirst(oDbKey, oDbDat, LockMode.DEFAULT);

				while (oOst == OperationStatus.SUCCESS) {
					Record oRec = StorageObjectFactory.newRecord(getResultClass(), oTbl);
					oDbEnt = oDbeb.entryToObject(oDbKey, oDbDat);
					oRec.setKey(oDbEnt.getKey());
					oRec.setValue(oDbEnt.getWrapped());
					oValue = oRec.apply(sIndexColumn);
					if (oValue != null)
						if (Bytes.compareTo(BytesConverter.toBytes(oValue, iType), byMin) >= 0) {
							if (Bytes.compareTo(BytesConverter.toBytes(oValue, iType), byMax) <= 0)
								oEst.add((R) oRec);
							break;
						}
					oOst = oPur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);
				} // wend

				oOst = oPur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);
				while (oOst == OperationStatus.SUCCESS) {
					Record oRec = StorageObjectFactory.newRecord(getResultClass(), oTbl);
					oDbEnt = oDbeb.entryToObject(oDbKey, oDbDat);
					oRec.setKey(oDbEnt.getKey());
					oRec.setValue(oDbEnt.getWrapped());
					oValue = oRec.apply(sIndexColumn);
					if (oValue != null) {
						if (Bytes.compareTo(BytesConverter.toBytes(oValue, iType), byMax) > 0)
							break;
						oEst.add((R) oRec);
					}
					oOst = oPur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);

				} // wend
				oPur.close();
				oPur = null;
			}

		} catch (DeadlockException dlxc) {
			if (DebugFile.trace) {
				DebugFile.writeln(dlxc.getClass().getName() + " " + dlxc.getMessage());
				try {
					DebugFile.writeln(StackTraceUtil.getStackTrace(dlxc));
				} catch (Exception ignore) {
				}
				DebugFile.decIdent();
			}
			throw new JDOException(dlxc.getMessage(), dlxc);
		} catch (Exception xcpt) {
			if (DebugFile.trace) {
				DebugFile.writeln(xcpt.getClass().getName() + " " + xcpt.getMessage());
				try {
					DebugFile.writeln(StackTraceUtil.getStackTrace(xcpt));
				} catch (Exception ignore) {
				}
				DebugFile.decIdent();
			}
			throw new JDOException(xcpt.getMessage(), xcpt);
		} finally {
			try {
				if (oPur != null)
					oPur.close();
			} catch (Exception ignore) {
			}
			try {
				if (oCur != null)
					oCur.close();
			} catch (Exception ignore) {
			}
		}

		return (RecordSet<R>) oEst;
	} // fetch

	// --------------------------------------------------------------------------

	public <R extends Record> RecordSet<R> fetch(final String sIndexColumn, Date dtIndexValueMin, Date dtIndexValueMax) throws JDOException {

		RecordSet<R> oEst;

		if (dtIndexValueMax == null)
			oEst = fetch(sIndexColumn, BytesConverter.toBytes(dtIndexValueMin, Types.TIMESTAMP),
					BytesConverter.toBytes(Long.MAX_VALUE, Types.BIGINT));
		else if (dtIndexValueMin == null)
			oEst = fetch(sIndexColumn, BytesConverter.toBytes(0l, Types.BIGINT),
					BytesConverter.toBytes(dtIndexValueMax, Types.TIMESTAMP));
		else
			oEst = fetch(sIndexColumn, BytesConverter.toBytes(dtIndexValueMin, Types.TIMESTAMP),
					BytesConverter.toBytes(dtIndexValueMax, Types.TIMESTAMP));

		return oEst;
	}

	// --------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, int maxrows, int offset, Param... params) throws JDOException {
		return (RecordSet<R>) fetchOrDelete(fetchGroup, maxrows, offset, false, params);
	}

	// --------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private <R extends Record> Object fetchOrDelete(FetchGroup fetchGroup, int maxrows, int offset, boolean delete, Param... params) throws JDOException {

		if (null == params)
			throw new JDOException("DBTable.fetchOrDelete() Params may not be null");

		final int nValues = params.length;

		ArrayList<byte[]> aKeys = null;

		if (1 == nValues) {
			if (delete) {
				delete(params[0]);
				return 1;
			} else {
				return fetch(fetchGroup, params[0].getName(), params[0].getValue(), maxrows, 0);
			}
		} else {

			String sValue;

			if (DebugFile.trace) {
				String sPairs = "";
				for (int nv = 0; nv < nValues; nv++) {
					if (params[nv].getValue() == null)
						sValue = "null";
					else if (params[nv].getValue() instanceof String)
						sValue = (String) params[nv].getValue();
					else
						sValue = params[nv].getValue().toString();
					sPairs += (nv == 0 ? "" : ",") + params[nv].getName() + ":" + sValue;
					if (!oInd.containsKey(params[nv].getName()))
						throw new JDOException("DBTable.fetchOrDelete() Column " + params[nv].getName() + " is not indexed");
					if (sValue == null)
						throw new JDOException("DBTable.fetchOrDelete() Column " + params[nv].getName() + " may not be null");
					if (sValue.indexOf('%') >= 0)
						throw new JDOException("DBTable.fetchOrDelete() " + params[nv].getName()
								+ " % wildcards are not allowed in join cursors");
					if (sValue.equalsIgnoreCase("null") || sValue.equalsIgnoreCase("is null")
							|| sValue.equalsIgnoreCase("is not null"))
						throw new JDOException("DBTable.fetchOrDelete() " + params[nv].getName()
								+ " IS [NOT] NULL conditional is not allowed in join cursors");
				} // next
				DebugFile.writeln("Begin DBTable.fetchOrDelete({" + sPairs + "}," + String.valueOf(maxrows) + ")");
				DebugFile.incIdent();
			}

			if (maxrows <= 0)
				throw new JDOException("Invalid value for max rows parameter " + String.valueOf(maxrows));

			DBEntityBinding oDbeb = new DBEntityBinding(getCatalog());
			DatabaseEntry oDbDat = new DatabaseEntry();
			DatabaseEntry oDbKey = new DatabaseEntry();
			ArrayListRecordSet<? extends Record> oEst = new ArrayListRecordSet<>(getResultClass());
			JoinCursor oJur = null;

			OperationStatus[] aOst = new OperationStatus[nValues];
			DBIndex[] aIdxs = new DBIndex[nValues];
			SecondaryCursor[] aCurs = new SecondaryCursor[params.length];
			int fetched = 0;
			int skipped = 0;
			
			try {

				for (int sc = 0; sc < nValues; sc++) {
					if (params[sc].getValue() instanceof String)
						sValue = (String) params[sc].getValue();
					else
						sValue = params[sc].getValue().toString();
					aIdxs[sc] = oInd.get(params[sc].getName());
					if (aIdxs[sc].isClosed())
						openIndex(params[sc].getName(), getDataSource().isTransactional());
					aCurs[sc] = aIdxs[sc].getCursor(getTransaction());
					aOst[sc] = aCurs[sc].getSearchKey(new DatabaseEntry(BytesConverter.toBytes(sValue, Types.VARCHAR)),
							new DatabaseEntry(), LockMode.DEFAULT);
				} // next

				oJur = getDatabase().join(aCurs, JoinConfig.DEFAULT);

				if (delete) {
					aKeys = new ArrayList<byte[]>(1000);
					while (oJur.getNext(oDbKey, oDbDat, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
						if (fetched+skipped>=offset) {
							aKeys.add(oDbeb.objectToKey(oDbeb.entryToObject(oDbKey, oDbDat)));
							fetched++;
						} else {
							skipped++;
						}
					} // wend
				} else {
					while (oJur.getNext(oDbKey, oDbDat, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
						if (fetched+skipped>=offset) {
							Record oRec = makeRecord(getResultClass(), oDbeb, oDbKey, oDbDat);
							oEst.add((R) oRec);
							fetched++;
						} else {
							skipped++;
						}					
					}
				}
				oJur.close();
				oJur = null;

				for (int sc = nValues - 1; sc >= 0; sc--) {
					aCurs[sc].close();
					aCurs[sc] = null;
					aIdxs[sc].close();
					aIdxs[sc] = null;
				}

				if (delete) {
					for (byte[] k : aKeys)
						delete(k, getTransaction());
				}

			} catch (DeadlockException dlxc) {
				if (DebugFile.trace) {
					DebugFile.writeln(dlxc.getClass().getName() + " " + dlxc.getMessage());
					try {
						DebugFile.writeln(StackTraceUtil.getStackTrace(dlxc));
					} catch (Exception ignore) {
					}
					DebugFile.decIdent();
				}
				throw new JDOException(dlxc.getMessage(), dlxc);
			} catch (Exception xcpt) {
				if (DebugFile.trace) {
					DebugFile.writeln(xcpt.getClass().getName() + " " + xcpt.getMessage());
					try {
						DebugFile.writeln(StackTraceUtil.getStackTrace(xcpt));
					} catch (Exception ignore) {
					}
					DebugFile.decIdent();
				}
				throw new JDOException(xcpt.getMessage(), xcpt);
			} finally {
				try {
					if (oJur != null)
						oJur.close();
				} catch (Exception ignore) {
				}
				for (int sc = nValues - 1; sc >= 0; sc--) {
					try {
						if (aCurs[sc] != null)
							aCurs[sc].close();
					} catch (Exception ignore) {
					}
					try {
						if (aIdxs[sc] != null)
							aIdxs[sc].close();
					} catch (Exception ignore) {
					}
				} // next
			}

			if (DebugFile.trace) {
				DebugFile.decIdent();
				DebugFile.writeln("End DBTable.fetchOrDelete()");
			}

			if (delete)
				return fetched;
			else
				return (RecordSet<R>) oEst;
		} // fi
	} // fetch

	// --------------------------------------------------------------------------

	@Override
	public void createIndex(String indexName, boolean unique, Using indexUsing, String... columns) throws JDOException {
		DBIndex oIdx = openIndex(columns[0], getDataSource().isTransactional());
		try {
			oIdx.close();
		} catch (DatabaseException e) {
			if (DebugFile.trace) DebugFile.writeln("DatabaseException "+e.getMessage());
			throw new JDOException(e.getMessage(), e);
		}
	}

	// --------------------------------------------------------------------------
	
	public DBIndex openIndex(String sColumnName, boolean useTransaction) throws JDOException {

		if (DebugFile.trace)
			DebugFile.writeln("openIndex(" + sColumnName + ")");

		DBIndex oIdx = oInd.get(sColumnName);
		SecondaryConfig oSec = new SecondaryConfig();
		oSec.setAllowCreate(true);
		oSec.setAllowPopulate(true);
		oSec.setSortedDuplicates(true);
		oSec.setType(DatabaseType.BTREE);
		oSec.setReadOnly(isReadOnly());

		if (DebugFile.trace)
			DebugFile.writeln("relation type is " + oIdx.getType());

		if (!isReadOnly()) {
			if (oIdx.getType().equals(Type.ONE_TO_ONE) || oIdx.getType().equals(Type.MANY_TO_ONE)) {
				ColumnDef oCol = getColumnByName(sColumnName);
				if (oCol == null)
					throw new JDOException("Column " + sColumnName + " not found at table " + name());
				if (DebugFile.trace)
					DebugFile.writeln("SecondaryConfig.setKeyCreator(DBDataSource.getKeyCreator("+getResultClass().getName()+", "+oTbl.getName()+", "+sColumnName+", "+ColumnDef.typeName(oCol.getType())+")");
				oSec.setKeyCreator(getDataSource().getKeyCreator(getResultClass(), oTbl, sColumnName, oCol.getType()));
			} else if (oIdx.getType().equals(Type.ONE_TO_MANY)
					|| oIdx.getType().equals(Type.MANY_TO_MANY)) {
				if (DebugFile.trace)
					DebugFile.writeln("SecondaryConfig.setMultiKeyCreator(DBDataSource.getMultiKeyCreator("+getResultClass().getName()+", "+oTbl.getName()+", "+sColumnName+")");
				oSec.setMultiKeyCreator(getDataSource().getMultiKeyCreator(getResultClass(), oTbl, sColumnName));
			} else {
				if (DebugFile.trace)
					DebugFile.writeln("Invalid relationship " + oIdx.getType());
				throw new JDOException("Invalid relationship " + oIdx.getType());
			}
		}

		try {

			final String sSecDbPath = getDataSource().getPath() + getDatabase().getDatabaseName() + "." + sColumnName + ".db";
			final String sSecIdxName = getDatabase().getDatabaseName() + "_" + oIdx.getName();
			Transaction oTrn = useTransaction ? getTransaction() : null;
			
			if (DebugFile.trace)
				DebugFile.writeln("DBIndex.open(openSecondaryDatabase(" + oTrn + ", " + sSecDbPath + ", "
						+ sSecIdxName + ", " + getDatabase() + ", " + oSec + ", " + String.valueOf(isReadOnly())
						+ "))");
			
			oIdx.open(getDataSource().getEnvironment().openSecondaryDatabase(oTrn, sSecDbPath, sSecIdxName, getDatabase(), oSec));
			if (DebugFile.trace)
				DebugFile.writeln("opened index " + sSecIdxName);

		} catch (DeadlockException dle) {
			if (DebugFile.trace)
				DebugFile.writeln("DeadlockException " + String.valueOf(dle.getErrno())
						+ " whilst opening secondary database " + dle.getMessage());
			throw new JDOException(dle.getMessage(), dle);
		} catch (DatabaseException dbe) {
			if (DebugFile.trace)
				DebugFile.writeln("DatabaseException " + String.valueOf(dbe.getErrno())
						+ " whilst opening secondary database " + dbe.getMessage());
			throw new JDOException(dbe.getMessage(), dbe);
		} catch (FileNotFoundException fnf) {
			if (DebugFile.trace)
				DebugFile.writeln("FileNotFoundException " + fnf.getMessage());
			throw new JDOException(fnf.getMessage(), fnf);
		}

		return oIdx;
	}

	// --------------------------------------------------------------------------

	public void dropIndex(final String sIndexColumn) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.dropIndex("+sIndexColumn+")");
			DebugFile.incIdent();
		}
		try {

			if (oInd.containsKey(sIndexColumn)) {
				DBIndex oIdx = oInd.get(sIndexColumn);
				oIdx.close();
				oInd.remove(sIndexColumn);
				if (DebugFile.trace)
					DebugFile.writeln("Database.remove("+
						getDataSource().getPath() + getDatabase().getDatabaseName() + "." + oIdx.getName() + ".db, " + 
						getDatabase().getDatabaseName() + "_" + oIdx.getName() +")");
				getDataSource().getEnvironment().removeDatabase(getTransaction(), getDataSource().getPath() + getDatabase().getDatabaseName() + "." + oIdx.getName() + ".db", getDatabase().getDatabaseName() + "_" + oIdx.getName());
				File oDbf = new File(getDataSource().getPath() + getDatabase().getDatabaseName() + "." + oIdx.getName() + ".db");
				if (oDbf.exists()) oDbf.delete();
				// Database.remove(getDataSource().getPath() + getDatabase().getDatabaseName() + "." + oIdx.getName() + ".db", getDatabase().getDatabaseName() + "_" + oIdx.getName(), null);
			} else {
				throw new JDOException("Index not found " + sIndexColumn);
			}

		} catch (DatabaseException dbe) {
			throw new JDOException(dbe.getMessage(), dbe);
		} catch (FileNotFoundException fnf) {
			throw new JDOException(fnf.getMessage(), fnf);
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.dropIndex()");
		}
	} // dropIndex

	// --------------------------------------------------------------------------

	@Override
	public int columnsCount() {
		return oTbl.getNumberOfColumns();
	}

	// --------------------------------------------------------------------------

	@Override
	public ColumnDef getColumnByName(String columnName) {
		return oTbl.getColumnByName(columnName);
	}

	// --------------------------------------------------------------------------

	@Override
	public int getColumnIndex(String columnName) {
		return oTbl.getColumnIndex(columnName);
	}

	// --------------------------------------------------------------------------

	@Override
	public String getTimestampColumnName() {
		return oTbl.getCreationTimestampColumnName();
	}

	// --------------------------------------------------------------------------

	@Override
	public void setTimestampColumnName(String columnName) throws IllegalArgumentException {
		oTbl.setCreationTimestampColumnName(columnName);
	}

	// --------------------------------------------------------------------------

	@Override
	public PrimaryKeyMetadata getPrimaryKey() {
		return oTbl.getPrimaryKeyMetadata();
	}

	// --------------------------------------------------------------------------

	@Override
	public void store(Stored oRec) throws JDOException {

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.store()");
			DebugFile.incIdent();
		}

		if (getDataSource() instanceof DBTableDataSource && oRec instanceof Record) {
			DBTableDataSource tableDataSrc = (DBTableDataSource) getDataSource();
			if (oChk == null)
				oChk = new StandardConstraintsChecker(new DBSequenceGenerator(getDataSource(), name()), new DBForeignKeyChecker(tableDataSrc));
			oChk.check(tableDataSrc, (Record) oRec);
		}

		if (DebugFile.trace)
			DebugFile.writeln("DBBucket.store(Stored, BytesConverter.toBytes("+oRec.getKey()+", "+ColumnDef.typeName(oTbl.getColumnByName(oTbl.getPrimaryKeyMetadata().getColumn()).getType())+")");
		
		store(oRec, BytesConverter.toBytes(oRec.getKey(), oTbl.getColumnByName(oTbl.getPrimaryKeyMetadata().getColumn()).getType()));

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.store()");
		}		
	}

	// --------------------------------------------------------------------------

	@Override
	public int update(Param[] values, Param[] where) throws JDOException {
		RecordSet<? extends Record> affected = fetch(null, Integer.MAX_VALUE, 0, where);
		for (Record row : affected) {
			for (Param param : values)
				row.put(param.getName(), param.getValue());
			store(row);
		}
		return affected.size();
	}

	// --------------------------------------------------------------------------
	
	@Override
	public int count(String indexColumnName, Object indexValue) throws JDOException {
		if (null == indexColumnName)
			throw new JDOException("DBTable.count() Column name may not be null");

		final boolean usingPk = indexColumnName.equalsIgnoreCase(getPrimaryKey().getColumn());
		
		if (!usingPk && !oInd.containsKey(indexColumnName))
			throw new JDOException(
					"DBTable.count() Column " + indexColumnName + " is not a primary key nor a secondary index");
				
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBTable.count(" + indexColumnName + "," + indexValue + ")");
			DebugFile.incIdent();
			DebugFile.writeln(usingPk ? "using primary key" : "using secondary index");
		}

		String sIndexValue;

		if (null == indexValue)
			sIndexValue = "";
		else if (indexValue instanceof String)
			sIndexValue = (String) indexValue;
		else
			sIndexValue = indexValue.toString();

		Cursor oPur = null;
		SecondaryCursor oCur = null;
		OperationStatus oOst;
		int fetched = 0;

		try {

			if (DebugFile.trace)
				DebugFile.writeln("new DBEntityBinding(getCatalog())");

			DBEntityBinding oDbeb = new DBEntityBinding(getCatalog());
			DatabaseEntry oDbDat = new DatabaseEntry();
			DatabaseEntry oDbKey = new DatabaseEntry();

			if (sIndexValue.equals("%") || sIndexValue.equalsIgnoreCase(Operator.ISNOTNULL)) {

				if (DebugFile.trace)
					DebugFile.writeln("Database.openCursor(getTransaction(),null)");

				oPur = getDatabase().openCursor(getTransaction(), null);

				oOst = oPur.getFirst(oDbKey, oDbDat, LockMode.DEFAULT);
				while (oOst == OperationStatus.SUCCESS) {
					Record oRec = makeRecord(getResultClass(), oDbeb, oDbKey, oDbDat);
					if (!oRec.isNull(indexColumnName))
						fetched++;
					oOst = oPur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);						
				} // wend
				oPur.close();
				oPur = null;

			} else if (sIndexValue.equalsIgnoreCase("NULL") || sIndexValue.equalsIgnoreCase(Operator.ISNULL)) {

				if (DebugFile.trace)
					DebugFile.writeln("Database.openCursor(getTransaction(),null)");

				oPur = getDatabase().openCursor(getTransaction(), null);
				oOst = oPur.getFirst(oDbKey, oDbDat, LockMode.DEFAULT);
				while (oOst == OperationStatus.SUCCESS) {
					Record oRec = makeRecord(getResultClass(), oDbeb, oDbKey, oDbDat);
					if (oRec.isNull(indexColumnName))
						fetched++;
					oOst = oPur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);
				} // wend
				oPur.close();
				oPur = null;

			} else {

				if (usingPk) {
					if (DebugFile.trace)
						DebugFile.writeln("Database.openCursor(getTransaction(),null)");
					oPur = getDatabase().openCursor(getTransaction(), null);
				} else {
					DBIndex oIdx;
					oIdx = oInd.get(indexColumnName);
					if (oIdx.isClosed())
						openIndex(indexColumnName, getDataSource().isTransactional());
					oCur = oIdx.getCursor(getTransaction());
				}

				if (sIndexValue.endsWith("%")) {
					sIndexValue = sIndexValue.substring(0, sIndexValue.length() - 1);
					oDbKey = new DatabaseEntry(BytesConverter.toBytes(sIndexValue, oTbl.getColumnByName(indexColumnName).getType()));
					if (usingPk)
						oOst = oPur.getSearchKeyRange(oDbKey, oDbDat, LockMode.DEFAULT);
					else
						oOst = oCur.getSearchKeyRange(oDbKey, oDbDat, LockMode.DEFAULT);

					while (oOst == OperationStatus.SUCCESS) {
						Record oRec = makeRecord(getResultClass(), oDbeb, oDbKey, oDbDat);
						try {
							oRec.getColumn(indexColumnName);
							if (oRec.getString(indexColumnName, "").startsWith(sIndexValue)) {
								fetched++;
								if (usingPk)
									oOst = oPur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);
								else
									oOst = oCur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);
							} else {
								oOst = OperationStatus.NOTFOUND;
							}
						} catch (ArrayIndexOutOfBoundsException columnnotfound) {
							oOst = OperationStatus.KEYEMPTY;
						}
					} // wend matches

				} else {

					if (DebugFile.trace)
						DebugFile.writeln("new DatabaseEntry(BytesConverter.toBytes(" + sIndexValue + ", " + ColumnDef.typeName(getColumnByName(indexColumnName).getType()) + "))");
					byte[] byKey = BytesConverter.toBytes(sIndexValue, getColumnByName(indexColumnName).getType());
					if (DebugFile.trace) {
						StringBuffer oStrKey = new StringBuffer(byKey.length*3);
						for (int b=0; b<byKey.length; b++) oStrKey.append(" "+Integer.toHexString(byKey[b]));
						DebugFile.writeln("raw key hex is"+oStrKey.toString());
					}
					oDbKey = new DatabaseEntry(byKey);
					if (usingPk) {
						if (DebugFile.trace)
							DebugFile.writeln("Database.get(" + sIndexValue + ", DatabaseEntry, LockMode.DEFAULT)");
						oOst = getDatabase().get(getTransaction(), oDbKey, oDbDat, LockMode.DEFAULT);
						if (DebugFile.trace)
							DebugFile.writeln("get return status was "+oOst);
						if (oOst == OperationStatus.SUCCESS) {
							fetched++;							
						}
					} else {
						if (DebugFile.trace)
							DebugFile.writeln("SecondaryCursor.getSearchKey(" + sIndexValue + ", DatabaseEntry, LockMode.DEFAULT)");
						oOst = oCur.getSearchKey(oDbKey, oDbDat, LockMode.DEFAULT);
						if (DebugFile.trace)
							DebugFile.writeln("getSearchKey return status was "+oOst);
						while (oOst == OperationStatus.SUCCESS) {
							fetched++;
							oOst = oCur.getNextDup(oDbKey, oDbDat, LockMode.DEFAULT);
							if (DebugFile.trace)
								DebugFile.writeln("getNextDup return status was "+oOst);
						} // wend
					}

				} // fi

				if (oPur != null) {
					oPur.close();
					oPur = null;
				}
				if (oCur != null) {
					oCur.close();
					oCur = null;
				}
			} // fi

		} catch (DeadlockException dlxc) {
			if (DebugFile.trace) {
				DebugFile.writeln(dlxc.getClass().getName() + " " + dlxc.getMessage());
				try {
					DebugFile.writeln(StackTraceUtil.getStackTrace(dlxc));
				} catch (Exception ignore) {
				}
				DebugFile.decIdent();
			}
			throw new JDOException(dlxc.getMessage(), dlxc);
		} catch (Exception xcpt) {
			if (DebugFile.trace) {
				DebugFile.writeln(xcpt.getClass().getName() + " " + xcpt.getMessage());
				try {
					DebugFile.writeln(StackTraceUtil.getStackTrace(xcpt));
				} catch (Exception ignore) {
				}
				DebugFile.decIdent();
			}
			throw new JDOException(xcpt.getMessage(), xcpt);
		} finally {
			try {
				if (oPur != null)
					oPur.close();
			} catch (Exception ignore) {
			}
			try {
				if (oCur != null)
					oCur.close();
			} catch (Exception ignore) {
			}
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBTable.count() : " + String.valueOf(fetched));
		}

		return fetched;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueFrom, Object valueTo) throws JDOException {
		if (valueFrom==null)
			throw new JDOUserException("Lower bound for range query is required");
		if (valueTo==null)
			throw new JDOUserException("Upper bound for range query is required");
		if (!valueFrom.getClass().equals(valueTo.getClass()))
			throw new IllegalArgumentException("Lower and upper bound type mismatch");
		if (valueFrom instanceof Date)
			return fetch(indexColumnName, (Date) valueFrom, (Date) valueTo);
		else if (valueFrom instanceof byte[])
			return fetch(indexColumnName, (byte[]) valueFrom, (byte[]) valueTo);
		else {
			int ctype = getColumnByName(indexColumnName).getType();
			return fetch(indexColumnName, BytesConverter.toBytes(valueFrom, ctype), BytesConverter.toBytes(valueTo, ctype));
		}
	}

	// --------------------------------------------------------------------------
	
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueFrom, Object valueTo, int maxrows, int offset) throws JDOException {
		throw new JDOUnsupportedOptionException("Range query with offset and limit not implemented");
	}

}
