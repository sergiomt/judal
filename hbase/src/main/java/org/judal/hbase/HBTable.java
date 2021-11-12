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

import java.io.IOException;

import java.util.Iterator;
import java.util.Set;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.metadata.PrimaryKeyMetadata;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.knowgate.debug.DebugFile;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;

import org.judal.serialization.BytesConverter;

import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.StorageObjectFactory;

public class HBTable extends HBSchemalessTable implements org.judal.storage.table.Table {

	private final HBTableDataSource oTDts;

	private Set<HBIterator> oItr;
	
	private String sTsc;

	// --------------------------------------------------------------------------
	
	public HBTable(HBTableDataSource oDts, Configuration oCfg, Record oRec) throws IOException {
		super(oDts, oCfg, oRec);
		oTDts = oDts;
		oItr = null;
		sTsc = null;
	}

	// --------------------------------------------------------------------------
	
	@Override
	public ColumnDef[] columns() {
		ColumnDef[] oLst;
		try {
			if (null==oTDts.getMetaData()) return null;
			oLst = oTDts.getMetaData().getColumns(name());
		} catch (Exception xcpt) {
			if (DebugFile.trace) DebugFile.writeln("HBTable.columns() "+xcpt.getClass().getName()+" "+xcpt.getMessage());
			oLst = null;
		}
		return oLst;
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
			Result oRes = getTable().get(oGet);
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
			DebugFile.writeln("Begin HBTable.store(" + oRow.getTableName() + "." + oRow.getKey() + ")");
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
						DebugFile.writeln(oRow.getClass().getName() + ".apply(" + oCol.getName() + ") == null");
				}
			}
			getTable().put(oPut);
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

		TableDef oDef = oTDts.getMetaData().getTable(name());
		R oRow;
		try {
			oRow = (R) StorageObjectFactory.newRecord(getCandidateClass(), oDef);
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
			oRst = StorageObjectFactory.newRecordSetOf((Class<R>) getCandidateClass(), maxrows);
		} catch (NoSuchMethodException e) {
			throw new JDOException(e.getMessage(), e);
		}

		Get oGet = new Get(BytesConverter.toBytes(valueSearched));
		try {
			Result oRes = getTable().get(oGet);
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

	// --------------------------------------------------------------------------

	@Override
	public void close(Iterator<Stored> iterator) {
		((HBIterator) iterator).close();
		oItr.remove(iterator);
	}

	// --------------------------------------------------------------------------

	@Override
	public void closeAll() {
		for (HBIterator oHbi : oItr)
			oHbi.close();
		oItr.clear();		
	}

	// --------------------------------------------------------------------------

	@Override
	public Iterator<Stored> iterator() {
		HBIterator oHbi = new HBIterator(this);
		oItr.add(oHbi);
		return oHbi;
	}

	// --------------------------------------------------------------------------

	@Override
	public int columnsCount() {
		return oTDts.getMetaData().getTable(name()).getNumberOfColumns();
	}

	// --------------------------------------------------------------------------

	@Override
	public ColumnDef getColumnByName(String columnName) {
		return oTDts.getMetaData().getTable(name()).getColumnByName(columnName);
	}

	// --------------------------------------------------------------------------

	@Override
	public int getColumnIndex(String columnName) {
		return oTDts.getMetaData().getTable(name()).getColumnIndex(columnName);
	}

	// --------------------------------------------------------------------------

	@Override
	public String getTimestampColumnName() {
		return sTsc;
	}

	// --------------------------------------------------------------------------

	@Override
	public void setTimestampColumnName(String columnName) throws IllegalArgumentException {
		sTsc = columnName;
	}

	// --------------------------------------------------------------------------

	@Override
	public PrimaryKeyMetadata getPrimaryKey() {
		return oTDts.getMetaData().getTable(name()).getPrimaryKeyMetadata();
	}

	// --------------------------------------------------------------------------

	@Override
	public long count(String indexColumnName, Object valueSearched) throws JDOException {
		if (!indexColumnName.equalsIgnoreCase(getPrimaryKey().getColumn()))
			throw new JDOUnsupportedOptionException("HBase only supports queries by primary key");
		return exists(valueSearched) ? 1 : 0;
	}

	// --------------------------------------------------------------------------

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup cols, String indexColumnName, Comparable<?> valueFrom, Comparable<?> valueTo, int maxrows, int offset) throws JDOException, IllegalArgumentException {

		if (!indexColumnName.equalsIgnoreCase(getPrimaryKey().getColumn()))
			throw new JDOUnsupportedOptionException("HBase only supports queries by primary key");

		RecordSet<R> rst;
		try {
			rst = StorageObjectFactory.newRecordSetOf((Class<R>) getCandidateClass(), maxrows);
		} catch (NoSuchMethodException e) {
			throw new JDOException(e.getMessage(), e);
		}
		TableDef tdef = oTDts.getMetaData().getTable(name());
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
			rsc = getTable().getScanner(scn);
			for (Result res=rsc.next(); res!=null && rowCount<maxrow; res=rsc.next()) {
				if (++rowCount>offset) {
					R row;
					try {
						row = (R) StorageObjectFactory.newRecord(getCandidateClass(), tdef);
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

}
