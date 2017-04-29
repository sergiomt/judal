package org.judal.cassandra;

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

import java.math.BigDecimal;

import java.sql.Types;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import javax.jdo.FetchGroup;
import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.metadata.PrimaryKeyMetadata;

import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.BigDecimalSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.FloatSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import static me.prettyprint.hector.api.factory.HFactory.createRangeSlicesQuery;

import static org.judal.cassandra.ColumnConverter.*;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.IndexDef.Using;
import org.judal.serialization.BytesConverter;
import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.Param;
import org.judal.storage.keyvalue.Stored;

public class ColumnFamily implements IndexableTable {

	private static final byte[] aNull = new byte[0];
	private static final String DefaultTimestampColumnName = "dt_created";

	private ColumnFamilyTemplate<String, String> oTemplate;
	private KeySpace oKeySpace;
	private String sName;
	private String sTimestampColumname;
	Class<? extends Record> oResultClass;
	Class<? extends Stored> oCandidateClass;
	
	public ColumnFamily(KeySpace oKsp, String sColumnFamily) {
		sName = sColumnFamily;
		oKeySpace = oKsp;
		oTemplate = new ThriftColumnFamilyTemplate<String, String>(oKsp.keySpace(), sColumnFamily, StringSerializer.get(), StringSerializer.get());
		sTimestampColumname = DefaultTimestampColumnName;
		oCandidateClass = null;
		oResultClass =  null;
	}

	@Override
	public String name() {
		return sName;
	}

	@Override
	public ColumnDef[] columns() {
		ColumnDef[] oCols = null;
		try {
			oCols = oKeySpace.getMetaData().getColumns(sName);
		} catch (ArrayIndexOutOfBoundsException neverthrown) {
		} catch (JDOException neverthrown) { }
		return oCols;
	}

	@Override
	public ColumnDef getColumnByName(String sColName) throws ArrayIndexOutOfBoundsException {
		for (ColumnDef c : columns())
			if (c.getName().equalsIgnoreCase(sColName))
				return c;
		throw new ArrayIndexOutOfBoundsException("Column "+sColName+" not found");
	}

	public TableDataSource getDataSource() {
		return oKeySpace;
	}

	@Override
	public void close() throws JDOException {
	}

	@Override
	public boolean exists(Object sKey) throws JDOException {
		boolean bExists;
		try {
			ColumnFamilyResult<String, String> oRes = oTemplate.queryColumns((String) sKey);
			bExists = oRes.hasResults();
		} catch (HectorException hcpt) {
			throw new JDOException(hcpt.getMessage(), hcpt);
		}
		return bExists;
	}

	@Override
	public boolean load(Object sKey, Stored oRec) throws JDOException {
		if (null==sKey)
			throw new NullPointerException("ColumrnFamily.load() key cannot be null");
		if (!(sKey  instanceof String))
			throw new NullPointerException("ColumnFamily.load() Cassandra can only use string type as primary key");
		if (((String) sKey).length()==0)
			throw new NullPointerException("ColumnFamily.load() key cannot be empty");
		Record oRow = (Record) oRec;
		boolean bFound = false;
		try {
			ColumnFamilyResult<String, String> oRes = oTemplate.queryColumns((String) sKey);
			bFound = oRes.hasResults();
			if (bFound) {
				for (ColumnDef oCol : oKeySpace.getMetaData().getColumns(name())) {
					String sCol = oCol.getName();
					byte[] byCol = oRes.getByteArray(sCol);
					if (byCol.length>0) {
						switch (oCol.getType()) {
						case Types.CHAR:
						case Types.NCHAR:
						case Types.VARCHAR:
						case Types.NVARCHAR:
						case Types.LONGVARCHAR:
						case Types.LONGNVARCHAR:
						case Types.CLOB:
							oRow.put(sCol, oRes.getString(sCol));
							break;							
						case Types.INTEGER:
						case Types.TINYINT:
							oRow.put(sCol, oRes.getInteger(sCol));
							break;
						case Types.BIGINT:
							oRow.put(sCol, oRes.getLong(sCol));
							break;
						case Types.DOUBLE:
							oRow.put(sCol, oRes.getDouble(sCol));
							break;
						case Types.FLOAT:
							oRow.put(sCol, oRes.getFloat(sCol));
							break;
						case Types.BOOLEAN:
							oRow.put(sCol, oRes.getBoolean(sCol));
							break;
						case Types.DATE:
						case Types.TIMESTAMP:
							oRow.put(sCol, oRes.getDate(sCol));
							break;
						case Types.DECIMAL:
						case Types.NUMERIC:
	            try {
	              oRow.put(sCol, (BigDecimal) BytesConverter.fromBytes(byCol, Types.DECIMAL));
              } catch (IOException ioe) {
                throw new JDOException(ioe.getMessage(), ioe);
              }
							break;
						case Types.BLOB:
						case Types.BINARY:
						case Types.VARBINARY:
						case Types.LONGVARBINARY:
						case Types.JAVA_OBJECT:
							oRow.put(sCol, byCol);
							break;
						default:
							throw new JDOException("Unsupported column type "+ColumnDef.typeName(oCol.getType()));
						}
					}
				}
			}
		} catch (HectorException hcpt) {
			throw new JDOException(hcpt.getMessage(), hcpt);
		}
		return bFound;
	} // load

	private void binParameter(String sCol, Object oObj, ColumnFamilyUpdater<String, String> oUpdt)
		throws JDOException {
		if (oObj instanceof String)
			oUpdt.setString(sCol, (String) oObj);
		else if (oObj instanceof Date)
			oUpdt.setDate(sCol, (Date) oObj);
		else if (oObj instanceof Integer)
			oUpdt.setInteger(sCol, (Integer) oObj);
		else if (oObj instanceof Long)
			oUpdt.setLong(sCol, (Long) oObj);
		else if (oObj instanceof Float)
			oUpdt.setFloat(sCol, (Float) oObj);
		else if (oObj instanceof Double)
			oUpdt.setDouble(sCol, (Double) oObj);
		else if (oObj instanceof Boolean)
			oUpdt.setBoolean(sCol, (Boolean) oObj);
		else if (oObj instanceof BigDecimal)
			oUpdt.setByteArray(sCol, BytesConverter.toBytes(oObj, Types.DECIMAL));
		else if (oObj instanceof byte[])
			oUpdt.setByteArray(sCol, (byte[]) oObj);
		else
			throw new JDOException("HBTable cannot bind parameter "+oObj.getClass().getName());		
	}

	@Override
	public void store(Stored oRow) throws JDOException {
		Record oRec = (Record) oRow;
		ColumnFamilyUpdater<String, String> oUpdt = oTemplate.createUpdater((String) oRec.getKey());
		for (ColumnDef oCol : oKeySpace.getMetaData().getColumns(name())) {
			String sCol = oCol.getName();
			Object oObj = oRec.apply(sCol);
			if (oRec.isNull(sCol))
				oUpdt.setValue(sCol, aNull, BytesArraySerializer.get());
			else
				binParameter(sCol, oObj, oUpdt);
		} // next
		try {
			oUpdt.addKey((String) oRec.getKey());
			oTemplate.update(oUpdt);
		} catch (NumberFormatException hcpt) {
			throw new JDOException(hcpt.getMessage(), hcpt);
		}
	}
	
	@Override
  public void insert(Param... aParams) throws JDOException {
	  String sPkValue = null;
	  for (Param oPar : aParams) {
	  	if (oPar.isPrimaryKey()) {
	  		sPkValue = (String) oPar.getValue();
	  		break;
	  	}
	  }
	  ColumnFamilyUpdater<String, String> oUpdt = oTemplate.createUpdater(sPkValue);
	  if (null==sPkValue)
	  	throw new JDOException("ColumnFamily.insert() cannoºt find primary key among parameters");

	  for (Param oCol : aParams) {
			String sCol = oCol.getName();
			Object oObj = oCol.getValue();
			if (oObj==null)
				oUpdt.setValue(sCol, aNull, BytesArraySerializer.get());
			else
				binParameter(sCol, oObj, oUpdt);
	   } // next
		try {
			oUpdt.addKey(sPkValue);
			oTemplate.update(oUpdt);
		} catch (NumberFormatException hcpt) {
			throw new JDOException(hcpt.getMessage(), hcpt);
		}
  }
	

	@Override
	public void delete(Object oKey) throws JDOException {
		try {
			oTemplate.deleteRow((String) oKey);
		} catch (HectorException hcpt) {
			throw new JDOException(hcpt.getMessage(), hcpt);
		}
	}

	@Override
	public int delete(Param[] oParam)
			throws NullPointerException, IllegalArgumentException, JDOException {

		if (oParam.length!=1)
			throw new JDOUserException("Cassandra only supports filtering by a single index value");
			
		StringSerializer s = StringSerializer.get();	  
		ArrayList<String> aKeys = new   ArrayList<String>();

		String sIndexColumn = oParam[0].getName();
		String sIndexValue = oParam[0].getValue().toString();
		
		switch (getColumnByName(sIndexColumn).getType()) {

		case Types.BIGINT:
			RangeSlicesQuery<String, String, Long> oLQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, LongSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().length)
			.setReturnKeysOnly()
			.addEqualsExpression(sIndexColumn, new Long(sIndexValue));
			for (Row<String, String, Long> oRow : oLQry.execute().get())
				aKeys.add(oRow.getKey());
			break;

		case Types.INTEGER:
			RangeSlicesQuery<String, String, Integer> oIQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, IntegerSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().length)
			.setReturnKeysOnly()
			.addEqualsExpression(sIndexColumn, new Integer(sIndexValue));
			for (Row<String, String, Integer> oRow : oIQry.execute().get())
				aKeys.add(oRow.getKey());
			break;

		case Types.DOUBLE:
			RangeSlicesQuery<String, String, Double> oDQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, DoubleSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().length)
			.setReturnKeysOnly()
			.addEqualsExpression(sIndexColumn, new Double(sIndexValue));
			for (Row<String, String, Double> oRow : oDQry.execute().get())
				aKeys.add(oRow.getKey());
			break;

		case Types.FLOAT:
			RangeSlicesQuery<String, String, Float> oFQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, FloatSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().length)
			.setReturnKeysOnly()
			.addEqualsExpression(sIndexColumn, new Float(sIndexValue));
			for (Row<String, String, Float> oRow : oFQry.execute().get())
				aKeys.add(oRow.getKey());
			break;

		case Types.DECIMAL:
		case Types.NUMERIC:
			RangeSlicesQuery<String, String, BigDecimal> oBQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, BigDecimalSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().length)
			.setReturnKeysOnly()
			.addEqualsExpression(sIndexColumn, new BigDecimal(sIndexValue));
			for (Row<String, String, BigDecimal> oRow : oBQry.execute().get())
				aKeys.add(oRow.getKey());
			break;

		default:
			RangeSlicesQuery<String, String, String> oSQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, s)
			.setColumnFamily(sName)
			.setRange("", "", false, columns().length)
			.setReturnKeysOnly()
			.addEqualsExpression(sIndexColumn, sIndexValue);
			for (Row<String, String, String> oRow : oSQry.execute().get())
				aKeys.add(oRow.getKey());
		}
		try {
			for (String k : aKeys)
				oTemplate.deleteRow(k);
		} catch (HectorException hcpt) {
			throw new JDOException(hcpt.getMessage(), hcpt);
		}
		return 1;
	}

	@Override
	public void dropIndex(String sIndexColumn) throws JDOException {
		throw new JDOUnsupportedOptionException("Drop Index is not supported by Cassandra Hector interface");
	}

	@Override
	public String getTimestampColumnName() {
		return sTimestampColumname;
	}

	@Override
	public void setTimestampColumnName(String columnName) throws IllegalArgumentException {
		sTimestampColumname = columnName;
	}

	@Override
	public PrimaryKeyMetadata getPrimaryKey() {
		return oKeySpace.getMetaData().getTable(name()).getPrimaryKeyMetadata();
	}

	@Override
	public Class<Stored> getCandidateClass() {
		return (Class<Stored>) oCandidateClass;
	}

	@Override
	public void setClass(Class<? extends Stored> candidateClass) {
		oCandidateClass = candidateClass;		
	}

	@Override
	public void close(Iterator<Stored> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void closeAll() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public FetchPlan getFetchPlan() {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int columnsCount() {
		return oKeySpace.getTableDef(name()).getNumberOfColumns();
	}

	@Override
	public int getColumnIndex(String columnName) {
		return oKeySpace.getTableDef(name()).getColumnIndex(columnName);
	}

	@Override
	public boolean exists(Param... keys) throws JDOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int count(String indexColumnName, Object valueSearched) throws JDOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched)
			throws JDOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched,
			int maxrows, int offset) throws JDOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueFrom,
			Object valueTo) throws JDOException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueFrom,
			Object valueTo, int maxrows, int offset) throws JDOException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends Record> getResultClass() {
		return oResultClass;
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, int maxrows, int offset, Param... params)
			throws JDOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Param[] values, Param[] where) throws JDOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void createIndex(String indexName, boolean unique, Using indexUsing, String... columns) throws JDOException {
		throw new JDOUnsupportedOptionException("Create Index is not supported by Cassandra Hector interface");
	}

	/*
	public RecordSet fetch(int iMaxRows, int iOffset) throws JDOException {
		HashMap<String, Integer> resultMap = new HashMap<String, Integer>();
		RowSlice oRetVal = new RowSlice();
		String lastKeyForMissing = "";
		StringSerializer s = StringSerializer.get();
		QueryResult<OrderedRows<String, String, String>> oRes;
		OrderedRows<String, String, String> oRows;
		Row<String, String, String> oLast;
		RangeSlicesQuery<String, String, String> oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, s);
		oQry.setColumnFamily(sName);
		oQry.setRange("", "", false, columns().length);

		if (iOffset>0) {
			oQry.setRowCount(iOffset);
			oQry.setKeys("", "");
			oRes = oQry.execute();
			oRows = oRes.get();
			oLast = oRows.peekLast();
			if (oLast!=null)
				lastKeyForMissing = oLast.getKey();
		}

		if (iMaxRows>0)
			oQry.setRowCount(iMaxRows);
		else
			oQry.setRowCount(2147483647);

		int rowCnt = 0;

		while (true) {
			
			oQry.setKeys(lastKeyForMissing, "");
			oRes = oQry.execute();
			oRows = oRes.get();

			oLast = oRows.peekLast();
			if (oLast!=null) {
				lastKeyForMissing = oLast.getKey();
				
			  for (Row<String, String, String> oRow : oRows) {
				  if (!resultMap.containsKey(oRow.getKey())) {
					  resultMap.put(oRow.getKey(), ++rowCnt);
					  oRetVal.add(convertHectorRowToStandardRowStr(new StandardRow (sName, columns()), oRow));
				  }
			  } // next
			}  else {
				lastKeyForMissing = "";			
			}

			if ((oRows.getCount()!=oQry.getRowCount()) || (oRows.getCount()==0)) break;

		} // wend

		return oRetVal;
	}

	public RecordSet fetch(String sIndexColumn, String sIndexValue)
			throws JDOException {
		return fetch (sIndexColumn, sIndexValue, -1);
	}

	public RecordSet fetch(String sIndexColumn, String sIndexValueMin, String sIndexValueMax) throws JDOException {
		RecordSet oRetVal = new RowSlice();
		StringSerializer s = StringSerializer.get();

		switch (column(sIndexColumn).getType()) {

		case Types.BIGINT:
			RangeSlicesQuery<String, String, Long> oLQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, LongSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().size());
			if (sIndexValueMin!=null)
				oLQry.addGteExpression(sIndexColumn, new Long(sIndexValueMin));
			if (sIndexValueMax!=null)
				oLQry.addLteExpression(sIndexColumn, new Long(sIndexValueMax));
			for (Row<String, String, Long> oRow : oLQry.execute().get())
				oRetVal.add(convertHectorRowToStandardRowLng(new StandardRow (sName, columns()), oRow));
			break;

		case Types.INTEGER:
			RangeSlicesQuery<String, String, Integer> oIQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, IntegerSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().size());
			if (sIndexValueMin!=null)
				oIQry.addGteExpression(sIndexColumn, new Integer(sIndexValueMin));
			if (sIndexValueMax!=null)
				oIQry.addLteExpression(sIndexColumn, new Integer(sIndexValueMax));
			for (Row<String, String, Integer> oRow : oIQry.execute().get())
				oRetVal.add(convertHectorRowToStandardRowInt(new StandardRow (sName, columns()), oRow));
			break;

		case Types.DOUBLE:
			RangeSlicesQuery<String, String, Double> oDQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, DoubleSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().size());
			if (sIndexValueMin!=null)
				oDQry.addGteExpression(sIndexColumn, new Double(sIndexValueMin));
			if (sIndexValueMax!=null)
				oDQry.addLteExpression(sIndexColumn, new Double(sIndexValueMax));
			for (Row<String, String, Double> oRow : oDQry.execute().get())
				oRetVal.add(convertHectorRowToStandardRowDbl(new StandardRow (sName, columns()), oRow));
			break;

		case Types.FLOAT:
			RangeSlicesQuery<String, String, Float> oFQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, FloatSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().size());
			if (sIndexValueMin!=null)
				oFQry.addGteExpression(sIndexColumn, new Float(sIndexValueMin));
			if (sIndexValueMax!=null)
				oFQry.addLteExpression(sIndexColumn, new Float(sIndexValueMax));
			for (Row<String, String, Float> oRow : oFQry.execute().get())
				oRetVal.add(convertHectorRowToStandardRowFlt(new StandardRow (sName, columns()), oRow));
			break;

		case Types.DECIMAL:
		case Types.NUMERIC:
			RangeSlicesQuery<String, String, BigDecimal> oBQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, BigDecimalSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().size());
			if (sIndexValueMin!=null)
				oBQry.addGteExpression(sIndexColumn, new BigDecimal(sIndexValueMin));
			if (sIndexValueMax!=null)
				oBQry.addLteExpression(sIndexColumn, new BigDecimal(sIndexValueMax));
			for (Row<String, String, BigDecimal> oRow : oBQry.execute().get())
				oRetVal.add(convertHectorRowToStandardRowDec(new StandardRow (sName, columns()), oRow));
			break;
			
		default:
			RangeSlicesQuery<String, String, String> oSQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, s)
			.setColumnFamily(sName)
			.setRange("", "", false, columns().size());
			if (sIndexValueMin!=null)
				oSQry.addGteExpression(sIndexColumn, sIndexValueMin);
			if (sIndexValueMax!=null)
				oSQry.addLteExpression(sIndexColumn, sIndexValueMax);
			for (Row<String, String, String> oRow : oSQry.execute().get())
				oRetVal.add(convertHectorRowToStandardRowStr(new StandardRow (sName, columns()), oRow));
		}
		return oRetVal;
	}

	public RecordSet fetch(String sIndexColumn, Date dtIndexValueMin, Date dtIndexValueMax) throws JDOException {
		RowSlice oRetVal = new RowSlice();
		StringSerializer s = StringSerializer.get();
		RangeSlicesQuery<String, String, Date> oDtQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, DateSerializer.get())
				.setColumnFamily(sName)
				.setRange("", "", false, columns().size());
		if (dtIndexValueMin!=null)	
			oDtQry.addGteExpression(sIndexColumn, dtIndexValueMin);
		if (dtIndexValueMax!=null)	
			oDtQry.addLteExpression(sIndexColumn, dtIndexValueMax);
		for (Row<String, String, Date> oRow : oDtQry.execute().get())
			oRetVal.add(convertHectorRowToStandardRowDate(new StandardRow (sName, columns()), oRow));
		return oRetVal;
	}

	public RecordSet fetch(String sIndexColumn, String sIndexValue, Collection<Column> oCols, int iMaxRows)
			throws JDOException {

		RecordSet oRetVal = new RowSlice();
		StringSerializer s = StringSerializer.get();

		if (oCols==null) oCols = columns();

		switch (column(sIndexColumn).getType()) {

		case Types.BIGINT:
			RangeSlicesQuery<String, String, Long> oLQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, LongSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().size())
			.addEqualsExpression(sIndexColumn, new Long(sIndexValue));
			if (iMaxRows>0) oLQry.setRowCount(iMaxRows);
			for (Row<String, String, Long> oRow : oLQry.execute().get())
				oRetVal.add(convertHectorRowToStandardRowLng(new StandardRow (sName, oCols), oRow));
			break;

		case Types.INTEGER:
			RangeSlicesQuery<String, String, Integer> oIQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, IntegerSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().size())
			.addEqualsExpression(sIndexColumn, new Integer(sIndexValue));
			if (iMaxRows>0) oIQry.setRowCount(iMaxRows);
			for (Row<String, String, Integer> oRow : oIQry.execute().get())
				oRetVal.add(convertHectorRowToStandardRowInt(new StandardRow (sName, oCols), oRow));
			break;

		case Types.DOUBLE:
			RangeSlicesQuery<String, String, Double> oDQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, DoubleSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().size())
			.addEqualsExpression(sIndexColumn, new Double(sIndexValue));
			if (iMaxRows>0) oDQry.setRowCount(iMaxRows);
			for (Row<String, String, Double> oRow : oDQry.execute().get())
				oRetVal.add(convertHectorRowToStandardRowDbl(new StandardRow (sName, oCols), oRow));
			break;

		case Types.FLOAT:
			RangeSlicesQuery<String, String, Float> oFQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, FloatSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().size())
			.addEqualsExpression(sIndexColumn, new Float(sIndexValue));
			if (iMaxRows>0) oFQry.setRowCount(iMaxRows);
			for (Row<String, String, Float> oRow : oFQry.execute().get())
				oRetVal.add(convertHectorRowToStandardRowFlt(new StandardRow (sName, oCols), oRow));
			break;

		case Types.DECIMAL:
		case Types.NUMERIC:			
			RangeSlicesQuery<String, String, BigDecimal> oBQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, BigDecimalSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columns().size())
			.addEqualsExpression(sIndexColumn, new BigDecimal(sIndexValue));
			if (iMaxRows>0) oBQry.setRowCount(iMaxRows);
			for (Row<String, String, BigDecimal> oRow : oBQry.execute().get())
				oRetVal.add(convertHectorRowToStandardRowDec(new StandardRow (sName, oCols), oRow));
			break;
			
		default:
			RangeSlicesQuery<String, String, String> oSQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, s)
			.setColumnFamily(sName)
			.setRange("", "", false, columns().size())
			.addEqualsExpression(sIndexColumn, sIndexValue);
			if (iMaxRows>0) oSQry.setRowCount(iMaxRows);
			for (Row<String, String, String> oRow : oSQry.execute().get())
				oRetVal.add(convertHectorRowToStandardRowStr(new StandardRow (sName, oCols), oRow));
		}
		return oRetVal;
	}

	public RecordSet fetch(String sIndexColumn, String sIndexValue, int iMaxRows)
			throws JDOException {
		return fetch(sIndexColumn, sIndexValue, null, iMaxRows);
	}

	public RecordSet fetch(String sIndexColumn, String sIndexValue, Collection<Column> oCols)
			throws JDOException {
		return fetch(sIndexColumn, sIndexValue, oCols, -1);
	}

	public RecordSet fetch(NameValuePair[] aPairs, int iMaxRows)
			throws JDOException {

		RecordSet oRetVal = new RowSlice();
		StringSerializer s = StringSerializer.get();

		RangeSlicesQuery<String, String, String> oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, s)
				.setColumnFamily(sName)
				.setRange("", "", false, columns().size());
		for (NameValuePair oNvp : aPairs)
			oQry.addEqualsExpression(oNvp.getName(), oNvp.getValue());
		if (iMaxRows>0) oQry.setRowCount(iMaxRows);
		for (Row<String, String, String> oRow : oQry.execute().get())
			oRetVal.add(convertHectorRowToStandardRowStr(new StandardRow (sName, columns()), oRow));
		return oRetVal;
	}

	@Override
	public RecordSet last(String sOrderByColumn, int iMaxRows, int iOffset)
			throws JDOException {
		throw new UnsupportedOperationException("Table.last() is not supported by Cassandra");
	}

	@Override
	public RecordSet last(String sOrderByColumn, Collection<Column> oCols, int iMaxRows, int iOffset)
			throws JDOException {
		throw new UnsupportedOperationException("Table.last() is not supported by Cassandra");
	}

	@Override
	public void truncate() throws JDOException {
		oKeySpace.getCluster().truncate(oKeySpace.getName(), sName);
	}

	@Override
  public int update(Param[] aValues, Param[] aWhere) throws JDOException,
      NullPointerException {
		throw new UnsupportedOperationException("Table.update() is not supported by CASSANDRA engine");

  }

	@Override
  public boolean exists(Param key) throws NullPointerException,
      IllegalArgumentException, JDOException {
	  // TODO Auto-generated method stub
	  return false;
  }
	*/
}
