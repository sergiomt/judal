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
import java.sql.Types;
import java.util.Iterator;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import javax.jdo.FetchGroup;
import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.PersistenceManager;
import javax.jdo.metadata.PrimaryKeyMetadata;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.knowgate.debug.DebugFile;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;
import org.judal.serialization.BytesConverter;
import org.judal.storage.TableDataSource;
import org.judal.storage.ArrayListRecordSet;
import org.judal.storage.Param;
import org.judal.storage.ReadOnlyBucket;
import org.judal.storage.Record;
import org.judal.storage.StorageObjectFactory;
import org.judal.storage.RecordSet;
import org.judal.storage.Stored;
import org.judal.storage.Table;

public class HBTable implements Table {

	private String sTsc;
	private HTable oTbl;
	private HBTableDataSource oCfg;
	private Class<? extends Record> oCls;
	private HashSet<HBIterator> oItr;

	// --------------------------------------------------------------------------
	
	public HBTable(HBTableDataSource hCfg, HTable hTbl, Class<? extends Record> cRecordClass) {
		oCfg = hCfg;
		oTbl = hTbl;
		sTsc = null;
		oItr = null;
		oCls = cRecordClass;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public String name() {
		try {
			return (String) BytesConverter.fromBytes(oTbl.getTableName(), Types.VARCHAR);
		} catch (IOException e) {
			return null;
		}
	}

	// --------------------------------------------------------------------------
	
	public HTable getTable() {
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
		return oCfg;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public void close() throws JDOException {
		try {
			oTbl.close();
			oCfg.openedTables().remove(oTbl);
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
			return oTbl.exists(new Get(BytesConverter.toBytes(key)));
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
			return oTbl.exists(new Get(BytesConverter.toBytes(keys[0])));
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
				KeyValue oKvl = oRes.getColumnLatest(BytesConverter.toBytes(oCol.getFamily()), BytesConverter.toBytes(oCol.getName()));
				if (oKvl!=null)
					if (oKvl.getValue()!=null)
						oRow.put(oCol.getName(), BytesConverter.fromBytes(oKvl.getValue(), oCol.getType()));
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

		// oRow.checkConstraints(getDataSource());
		
		final byte[] byPK = BytesConverter.toBytes(oRow.getKey());
		Put oPut = new Put(byPK);
		try {
			for (ColumnDef oCol : columns()) {
				Object oObj = oRow.apply(oCol.getName());
				if (oObj!=null) {
					oPut.add(BytesConverter.toBytes(oCol.getFamily()), BytesConverter.toBytes(oCol.getName()), BytesConverter.toBytes(oObj, oCol.getType()));
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
					oPut.add(BytesConverter.toBytes(oPar.getFamily()), BytesConverter.toBytes(oPar.getName()), BytesConverter.toBytes(oObj, oPar.getType()));
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
		if (fetchGroup==null)
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

		ArrayListRecordSet<R> oRst = new ArrayListRecordSet<R>((Class<R>) oCls);
		Get oGet = new Get(BytesConverter.toBytes(valueSearched));
		try {
			Result oRes = oTbl.get(oGet);
			if (oRes!=null) {
				if (!oRes.isEmpty()) {
					for (String sColName : members) {
						ColumnDef oCol = getColumnByName(sColName);
						KeyValue oKvl = oRes.getColumnLatest(BytesConverter.toBytes(oCol.getFamily()), BytesConverter.toBytes(oCol.getName()));
						if (oKvl!=null) {
							if (oKvl.getValue()!=null) {
								oRow.put(oCol.getName(), BytesConverter.fromBytes(oKvl.getValue(), oCol.getType()));
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
	public void setClass(Class<Stored> candidateClass) {
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

	@Override
	public Class<Stored> getCandidateClass() {
		return (Class<Stored>) oCls;
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
	public int count(String indexColumnName, Object valueSearched) throws JDOException {
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

		ArrayListRecordSet<R> rst = new ArrayListRecordSet<R>((Class<R>) oCls);
		TableDef tdef = getDataSource().getMetaData().getTable(name());
		Scan scn = new Scan();
		scn.setStartRow(BytesConverter.toBytes(valueFrom));
		scn.setStopRow (BytesConverter.toBytes(valueTo));
		for (Object colName : cols.getMembers()) {
			ColumnDef cdef = getColumnByName((String) colName);
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
					for (ColumnDef oCol : columns()) {
						KeyValue oKvl = res.getColumnLatest(BytesConverter.toBytes(oCol.getFamily()), BytesConverter.toBytes(oCol.getName()));
						if (oKvl!=null) {
							if (oKvl.getValue()!=null)
								row.put(oCol.getName(), BytesConverter.fromBytes(oKvl.getValue(), oCol.getType()));
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

}
