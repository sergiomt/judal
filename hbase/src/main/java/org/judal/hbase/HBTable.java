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

import java.io.IOException;

import java.util.Iterator;
import java.util.List;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.jdo.FetchGroup;
import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.PersistenceManager;
import javax.jdo.metadata.PrimaryKeyMetadata;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.knowgate.debug.DebugFile;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;

import org.judal.serialization.BytesConverter;

import org.judal.storage.keyvalue.ReadOnlyBucket;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.Param;
import org.judal.storage.StorageObjectFactory;

public class HBTable implements org.judal.storage.table.Table {

	private String sTsc;
	private ClusterConnection oCon;
	private org.apache.hadoop.hbase.client.Table oTbl;
	private HBTableDataSource oDts;
	private Class<? extends Record> oCls;
	private HashSet<HBIterator> oItr;

	// --------------------------------------------------------------------------
	
	public HBTable(HBTableDataSource oDts, Configuration oCfg, Record oRec) throws IOException {
		this.oDts = oDts;
		this.sTsc = null;
		this.oItr = null;
		this.oCls = oRec.getClass();
		this.oCon = (ClusterConnection) ConnectionFactory.createConnection(oCfg);
		if (null==oCon)
			throw new IOException("HBase ConnectionFactory.createConnection() failed");
		this.oTbl = oCon.getTable(TableName.valueOf(oRec.getTableName()));
	}

	// --------------------------------------------------------------------------
	
	@Override
	public String name() {
		return oTbl.getName().getNameAsString();
	}

	// --------------------------------------------------------------------------
	
	public org.apache.hadoop.hbase.client.Table getTable() {
		return oTbl;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public ColumnDef[] columns() {
		ColumnDef[] oLst;
		try {
			if (null==getDataSource().getMetaData()) return null;
			oLst = getDataSource().getMetaData().getColumns(name());
		} catch (Exception xcpt) {
			if (DebugFile.trace) DebugFile.writeln("HBTable.columns() "+xcpt.getClass().getName()+" "+xcpt.getMessage());  	  
			oLst = null;
		}
		return oLst;
	}

	// --------------------------------------------------------------------------
	
	public TableDataSource getDataSource() {
		return oDts;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public void close() throws JDOException {
		try {
			oTbl.close();
			oCon.close();
			oDts.openedTables().remove(this);
		} catch (IOException ioe) {
			throw new JDOException(ioe.getMessage(),ioe);
		}
	}

	// --------------------------------------------------------------------------

	@Override
	public boolean exists(Object key)
		throws NullPointerException, IllegalArgumentException, JDOException {
		Object value;
		if (key==null) throw new NullPointerException("HBTable.exists() Key cannot be null");
		if (key instanceof Param)
			value = ((Param) key).getValue();
		else
			value = key;
		if (value==null) throw new NullPointerException("HBTable.exists() Key value cannot be null");		
		try {
			return oTbl.exists(new Get(BytesConverter.toBytes(value)));
		} catch (IOException ioe) {
			throw new JDOException(ioe.getMessage(),ioe);
		}
	}

	// --------------------------------------------------------------------------
	
	@Override
	public boolean exists(Param... keys) throws JDOException {
		if (keys.length>1)
			throw new JDOUnsupportedOptionException("HBase can only use a single column as index at a time");
		if (keys[0].getValue()==null) throw new NullPointerException("HBTable.exists() Key value cannot be null");
		try {
			return oTbl.exists(new Get(BytesConverter.toBytes(keys[0].getValue())));
		} catch (IOException ioe) {
			throw new JDOException(ioe.getMessage(),ioe);
		}
	}

	// --------------------------------------------------------------------------
	
	@Override
	public boolean load(Object key, Stored target) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin HBTable.load("+key+")");
			DebugFile.incIdent();
		}
		boolean retval = true;
		Record oRow = (Record) target;
		Get oGet = new Get(BytesConverter.toBytes(key));
		try {
			Result oRes = oTbl.get(oGet);
			for (ColumnDef oCol : columns()) {
				Cell oKvl = oRes.getColumnLatestCell(BytesConverter.toBytes(oCol.getFamily()), BytesConverter.toBytes(oCol.getName()));
				if (oKvl!=null) {
					byte[] colValue = getColumnLatestValue(oKvl);
					if (colValue!=null)
						oRow.put(oCol.getName(), BytesConverter.fromBytes(colValue, oCol.getType()));
				}
				if (DebugFile.trace) {
					if (oKvl==null)
						DebugFile.writeln("Result.getColumnLatest("+oCol.getFamily()+","+oCol.getName()+") == null");
					else if (oKvl.getValueArray()==null)
						DebugFile.writeln("Result.getColumnLatest("+oCol.getFamily()+","+oCol.getName()+").getValue("+ColumnDef.typeName(oCol.getType())+") is null");
					else
						DebugFile.writeln("Result.getColumnLatest("+oCol.getFamily()+","+oCol.getName()+").getValue("+ColumnDef.typeName(oCol.getType())+") == " + BytesConverter.fromBytes(getColumnLatestValue(oKvl), oCol.getType()));
				}
			}
			if (DebugFile.trace) {
				DebugFile.decIdent();
				DebugFile.writeln("End HBTable.load()");
			}
		} catch (IOException ioe) {
			if (DebugFile.trace) {
				DebugFile.decIdent();
				DebugFile.writeln("IOException "+ioe.getMessage());
			}
			throw new JDOException(ioe.getMessage(),ioe);
		}
		return retval;
	}

	// --------------------------------------------------------------------------

	@Override
	public void store(Stored oStored) throws JDOException {
		
		Record oRow = (Record) oStored;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin HBTable.store("+oRow.getTableName()+"."+oRow.getKey()+")");
			DebugFile.incIdent();
		}

		if (null==oRow.getKey()) {
			if (DebugFile.trace) {
				DebugFile.writeln("ERROR: Tried to store an HBase row which primary key is null");
				DebugFile.decIdent();
			}
			throw new JDOException("Can store an HBase row which primary key is null");
		}

		// oRow.checkConstraints(getDataSource());

		final byte[] byPK = BytesConverter.toBytes(oRow.getKey());
		Put oPut = new Put(byPK);
		try {
			for (ColumnDef oCol : columns()) {
				Object oObj = oRow.apply(oCol.getName());
				if (oObj!=null) {
					if (DebugFile.trace) {
						DebugFile.writeln("Put.add("+oCol.getFamily()+","+oCol.getName()+",toBytes("+oRow.getClass().getName()+".apply("+oCol.getName()+"),"+ColumnDef.typeName(oCol.getType())+"))");
						DebugFile.writeln(oObj.getClass().getName() + " " + oCol.getName()+"="+oObj.toString());
					}
					oPut.addColumn(BytesConverter.toBytes(oCol.getFamily()), BytesConverter.toBytes(oCol.getName()), BytesConverter.toBytes(oObj, oCol.getType()));

				} else {
					if (DebugFile.trace)
						DebugFile.writeln(oRow.getClass().getName()+".apply("+oCol.getName()+") == null");
				}
			}
			oTbl.put(oPut);
		} catch (IOException ioe) {
			if (DebugFile.trace) {
				DebugFile.writeln("IOException "+ioe.getMessage());
				DebugFile.decIdent();
			}
			throw new JDOException(ioe.getMessage(),ioe);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End HBTable.store()");
		}

	}

	// --------------------------------------------------------------------------
	
	@Override
	public void insert(Param... aParams) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin HBTable.insert(Param...)");
			DebugFile.incIdent();
		}

		// oRow.checkConstraints(getDataSource());
		
		byte[] byPK = null;
		for (Param oPar : aParams) {
			if (oPar.isPrimaryKey()) {
				byPK = BytesConverter.toBytes(oPar.getValue());
				break;
			}
		}
		if (null==byPK)
			throw new JDOException("No value supplied for primary key among insert parameters");
		Put oPut = new Put(byPK);
		try {
			for (Param oPar : aParams) {
				Object oObj = oPar.getValue();
				if (oObj!=null) {
					oPut.addColumn(BytesConverter.toBytes(oPar.getFamily()), BytesConverter.toBytes(oPar.getName()), BytesConverter.toBytes(oObj, oPar.getType()));
				}
			}
			oTbl.put(oPut);
		} catch (IOException ioe) {
			if (DebugFile.trace) {
				DebugFile.writeln("IOException "+ioe.getMessage());
				DebugFile.decIdent();
			}
			throw new JDOException(ioe.getMessage(),ioe);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End HBTable.insert()");
		}
	}
	
	// --------------------------------------------------------------------------
	
	@Override
  public void delete(Object oKey)
  	throws NullPointerException, IllegalArgumentException, JDOException {
		
		if (oKey==null) throw new NullPointerException("HBTable.delete() Key cannot be null");
		Object oVal;
		if (oKey instanceof Param)
			oVal = ((Param) oKey).getValue();
		else
			oVal = oKey;
		if (oVal==null) throw new NullPointerException("HBTable.delete() Key value cannot be null");

		Delete oDel = new Delete(BytesConverter.toBytes(oVal));
		try {
			oTbl.delete(oDel);
		} catch (IOException ioe) {
			throw new JDOException(ioe.getMessage(),ioe);
		}
  }

	// --------------------------------------------------------------------------

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched) throws JDOException {
		return fetch (fetchGroup, indexColumnName, valueSearched, Integer.MAX_VALUE, 0);
	}

	// --------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched, int maxrows, int offset) throws JDOException {

		if (!indexColumnName.equalsIgnoreCase(getPrimaryKey().getColumn()))
			throw new JDOUnsupportedOptionException("HBase only supports queries by primary key");
		if (valueSearched==null)
			throw new NullPointerException("HBTable.fetch("+indexColumnName+") index value cannot be null");
		if (fetchGroup==null)
			throw new NullPointerException("HBTable.fetch("+indexColumnName+") columns list cannot be null");
		else if (fetchGroup.getMembers().size()==0)
			throw new NullPointerException("HBTable.fetch("+indexColumnName+") columns list cannot be empty");

		TableDef oDef = getDataSource().getMetaData().getTable(name());
		R oRow;
		try {
			oRow = (R) StorageObjectFactory.newRecord(oCls, oDef);
		} catch (NoSuchMethodException nsme) {
			throw new JDOException(nsme.getMessage(), nsme);
		}
		Set<String> members;
		if (null==fetchGroup)
			members = oRow.fetchGroup().getMembers();
		else
			members = fetchGroup.getMembers();

		if (DebugFile.trace) {
			String sColList = "";
			for (String sCol : members)
				sColList+=","+sCol;
			DebugFile.writeln("Begin HBTable.fetch("+indexColumnName+","+valueSearched+",["+sColList.substring(1)+"])");
			DebugFile.incIdent();
		}

		RecordSet<R> oRst = null;

		try {
			oRst = StorageObjectFactory.newRecordSetOf((Class<R>) oCls, maxrows);
		} catch (NoSuchMethodException e) {
			throw new JDOException(e.getMessage(), e);
		}

		Get oGet = new Get(BytesConverter.toBytes(valueSearched));
		try {
			Result oRes = oTbl.get(oGet);
			if (oRes!=null) {
				if (!oRes.isEmpty()) {
					for (String sColName : members) {
						ColumnDef oCol = getColumnByName(sColName);
						Cell oKvl = oRes.getColumnLatestCell(BytesConverter.toBytes(oCol.getFamily()), BytesConverter.toBytes(oCol.getName()));
						if (oKvl!=null) {
							byte[] colValue = getColumnLatestValue(oKvl);
							if (colValue!=null) {
								oRow.put(oCol.getName(), BytesConverter.fromBytes(colValue, oCol.getType()));
							} else {
								if (DebugFile.trace) DebugFile.writeln("Value is null");
							}
						} else {
							if (DebugFile.trace) DebugFile.writeln("KeyValue is null");
						}
					}
					oRst.add(oRow);
				} else {
					if (DebugFile.trace) DebugFile.writeln("Result is empty");
				}
			} else {
				if (DebugFile.trace) DebugFile.writeln("Result is null");
			}
		} catch (IOException ioe) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new JDOException(ioe.getMessage(),ioe);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End HBTable.fetch() : "+String.valueOf(oRst.size()));
		}

		return oRst;
	}

	public <R extends Record> RecordSet<R> last(FetchGroup cols, int maxrows, int offset, String orderByValue)
			throws JDOException {
		return fetch(cols, getPrimaryKey().getColumn(), orderByValue+"00000000000000000000000000000000", orderByValue+"99999999999999999999999999999999", ReadOnlyBucket.MAX_ROWS, 0);
	}

	@Override
	public void setClass(Class<? extends Stored> candidateClass) {
		oCls = (Class<? extends Record>) candidateClass;
	}

	@Override
	public void close(Iterator<Stored> iterator) {
		((HBIterator) iterator).close();
		oItr.remove(iterator);
	}

	@Override
	public void closeAll() {
		for (HBIterator oHbi : oItr)
			oHbi.close();
		oItr.clear();		
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class getCandidateClass() {
		return oCls;
	}

	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		return null;
	}

	@Override
	public boolean hasSubclasses() {
		return false;
	}

	@Override
	public Iterator<Stored> iterator() {
		HBIterator oHbi = new HBIterator(this);
		oItr.add(oHbi);
		return oHbi;
	}

	@Override
	public int columnsCount() {
		return getDataSource().getMetaData().getTable(name()).getNumberOfColumns();
	}

	@Override
	public ColumnDef getColumnByName(String columnName) {
		return getDataSource().getMetaData().getTable(name()).getColumnByName(columnName);
	}

	@Override
	public int getColumnIndex(String columnName) {
		return getDataSource().getMetaData().getTable(name()).getColumnIndex(columnName);
	}

	@Override
	public Class<? extends Record> getResultClass() {
		return oCls;
	}

	@Override
	public String getTimestampColumnName() {
		return sTsc;
	}

	@Override
	public void setTimestampColumnName(String columnName) throws IllegalArgumentException {
		sTsc = columnName;
	}

	@Override
	public PrimaryKeyMetadata getPrimaryKey() {
		return getDataSource().getMetaData().getTable(name()).getPrimaryKeyMetadata();
	}

	@Override
	public long count(String indexColumnName, Object valueSearched) throws JDOException {
		if (!indexColumnName.equalsIgnoreCase(getPrimaryKey().getColumn()))
			throw new JDOUnsupportedOptionException("HBase only supports queries by primary key");
		return exists(valueSearched) ? 1 : 0;
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup cols, String indexColumnName, Object valueFrom, Object valueTo) throws JDOException, IllegalArgumentException {
		return fetch(cols, indexColumnName, valueFrom, valueTo, ReadOnlyBucket.MAX_ROWS, 0);
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup cols, String indexColumnName, Object valueFrom, Object valueTo, int maxrows, int offset) throws JDOException, IllegalArgumentException {

		if (!indexColumnName.equalsIgnoreCase(getPrimaryKey().getColumn()))
			throw new JDOUnsupportedOptionException("HBase only supports queries by primary key");

		RecordSet<R> rst;
		try {
			rst = StorageObjectFactory.newRecordSetOf((Class<R>) oCls, maxrows);
		} catch (NoSuchMethodException e) {
			throw new JDOException(e.getMessage(), e);
		}
		TableDef tdef = getDataSource().getMetaData().getTable(name());
		ColumnDef[] fetchCols = new ColumnDef[cols.getMembers().size()];
		Scan scn = new Scan();
		scn.setStartRow(BytesConverter.toBytes(valueFrom));
		scn.setStopRow (BytesConverter.toBytes(valueTo));		
		int c = 0;
		for (Object colName : cols.getMembers()) {
			ColumnDef cdef = getColumnByName((String) colName);
			fetchCols[c++] = cdef;
			scn.addColumn(BytesConverter.toBytes(cdef.getFamily()), BytesConverter.toBytes(cdef.getName()));
		}
		ResultScanner rsc = null;
		int rowCount = 0;
		final int maxrow = maxrows+offset;
		try {
			rsc = oTbl.getScanner(scn);
			for (Result res=rsc.next(); res!=null && rowCount<maxrow; res=rsc.next()) {
				if (++rowCount>offset) {
					R row;
					try {
						row = (R) StorageObjectFactory.newRecord(oCls, tdef);
					} catch (NoSuchMethodException nsme) {
						throw new JDOException(nsme.getMessage(), nsme);
					}
					for (ColumnDef oCol : fetchCols) {
						byte[] columnValue = getColumnLatestValue(res, oCol.getFamily(), oCol.getName());
						if (columnValue!=null) {
								row.put(oCol.getName(), BytesConverter.fromBytes(columnValue, oCol.getType()));
						}
				  } // next
					rst.add(row);
				} // fi (nRowCount>iOffset)
			} // next
		} catch (IOException ioe) {
			throw new JDOException(ioe.getMessage(),ioe);
		} finally {
			if(rsc!=null) rsc.close();
		}
		return rst;
	}

	public int update(Param[] aValues, Param[] aWhere) throws JDOException {
		if (aWhere==null) throw new NullPointerException("HBTable.update() where clause cannot be null");
		if (aWhere.length!=1) throw new IllegalArgumentException("HBTable updates must use exactly one parameter");
		Put oPut = new Put(BytesConverter.toBytes((String) aWhere[0].getValue()));
		try {
			for (Param v : aValues) {
				if (v.getValue()==null)
					oPut.addColumn(BytesConverter.toBytes(v.getFamily()), BytesConverter.toBytes(v.getName()), new byte[0]);
				else
					oPut.addColumn(BytesConverter.toBytes(v.getFamily()), BytesConverter.toBytes(v.getName()), BytesConverter.toBytes(v.getValue(), v.getType()));
			}
			oTbl.put(oPut);
		} catch (IOException ioe) {
			throw new JDOException(ioe.getMessage(), ioe);
		}
		return 1;
	}

	private byte[] getColumnLatestValue(Cell oCll) {
		final byte[] valueArray = oCll.getValueArray();
		return valueArray==null ? null :Arrays.copyOfRange(valueArray, oCll.getValueOffset(), valueArray.length);
	}

	private byte[] getColumnLatestValue(Result oRes, String sFamily, String sQualifier) {
		Cell oCll = oRes.getColumnLatestCell(BytesConverter.toBytes(sFamily), BytesConverter.toBytes(sQualifier));
		return null==oCll ? null : getColumnLatestValue(oCll);
	}

}
