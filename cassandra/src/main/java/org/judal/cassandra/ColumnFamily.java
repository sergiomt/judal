package org.judal.cassandra;

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
import java.lang.reflect.Constructor;
import java.math.BigDecimal;

import java.sql.Types;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import static me.prettyprint.hector.api.factory.HFactory.createRangeSlicesQuery;

import static org.judal.cassandra.ColumnConverter.*;

import static com.knowgate.typeutils.ObjectFactory.filterParameters;
import static com.knowgate.typeutils.ObjectFactory.getConstructor;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.IndexDef.Using;
import org.judal.metadata.TableDef;
import org.judal.serialization.BytesConverter;

import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.Param;
import org.judal.storage.keyvalue.Stored;

import static org.judal.storage.StorageObjectFactory.newRecord;
import static org.judal.storage.StorageObjectFactory.newRecordSetOf;

public class ColumnFamily implements IndexableTable {

	private static final int DEFAULT_INITIAL_RECORDSET_SIZE = 100;

	private static final byte[] aNull = new byte[0];
	private static final String DefaultTimestampColumnName = "dt_created";

	private ColumnFamilyTemplate<String, String> oTemplate;
	private KeySpace oKeySpace;
	private String sName;
	private String sTimestampColumname;
	private Constructor<? extends Record> oRecConstructor;
	private Class<? extends Record> oResultClass;
	private Class<? extends Stored> oCandidateClass;

	public ColumnFamily(KeySpace oKsp, String sColumnFamily) {
		sName = sColumnFamily;
		oKeySpace = oKsp;
		oTemplate = new ThriftColumnFamilyTemplate<String, String>(oKsp.keySpace(), sColumnFamily, StringSerializer.get(), StringSerializer.get());
		sTimestampColumname = DefaultTimestampColumnName;
		oCandidateClass = null;
		oResultClass =  null;
		oRecConstructor = null;
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
			.setRange("", "", false, columnsCount())
			.setReturnKeysOnly()
			.addEqualsExpression(sIndexColumn, new Long(sIndexValue));
			for (Row<String, String, Long> oRow : oLQry.execute().get())
				aKeys.add(oRow.getKey());
			break;

		case Types.INTEGER:
			RangeSlicesQuery<String, String, Integer> oIQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, IntegerSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columnsCount())
			.setReturnKeysOnly()
			.addEqualsExpression(sIndexColumn, new Integer(sIndexValue));
			for (Row<String, String, Integer> oRow : oIQry.execute().get())
				aKeys.add(oRow.getKey());
			break;

		case Types.DOUBLE:
			RangeSlicesQuery<String, String, Double> oDQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, DoubleSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columnsCount())
			.setReturnKeysOnly()
			.addEqualsExpression(sIndexColumn, new Double(sIndexValue));
			for (Row<String, String, Double> oRow : oDQry.execute().get())
				aKeys.add(oRow.getKey());
			break;

		case Types.FLOAT:
			RangeSlicesQuery<String, String, Float> oFQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, FloatSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columnsCount())
			.setReturnKeysOnly()
			.addEqualsExpression(sIndexColumn, new Float(sIndexValue));
			for (Row<String, String, Float> oRow : oFQry.execute().get())
				aKeys.add(oRow.getKey());
			break;

		case Types.DECIMAL:
		case Types.NUMERIC:
			RangeSlicesQuery<String, String, BigDecimal> oBQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, BigDecimalSerializer.get())
			.setColumnFamily(sName)
			.setRange("", "", false, columnsCount())
			.setReturnKeysOnly()
			.addEqualsExpression(sIndexColumn, new BigDecimal(sIndexValue));
			for (Row<String, String, BigDecimal> oRow : oBQry.execute().get())
				aKeys.add(oRow.getKey());
			break;

		default:
			RangeSlicesQuery<String, String, String> oSQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, s)
			.setColumnFamily(sName)
			.setRange("", "", false, columnsCount())
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

	@SuppressWarnings("unchecked")
	@Override
	public boolean exists(Param... keys) throws JDOException {
		if (keys.length!=1)
			throw new JDOUserException("Cassandra only supports filtering by a single index value");

		String lastKeyForMissing = "";
		StringSerializer s = StringSerializer.get();
		QueryResult<?> oRes;
		OrderedRows<String, String, ?> oRows;
		Row<String, String, String> oLast;
		RangeSlicesQuery<String, String, ?> oQry;
		switch (keys[0].getType()) {

		case Types.BIGINT:
			oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, LongSerializer.get());
			break;

		case Types.INTEGER:
			oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, IntegerSerializer.get());
			break;

		case Types.DOUBLE:
			oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, DoubleSerializer.get());
			break;

		case Types.FLOAT:
			oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, FloatSerializer.get());
			break;

		case Types.DECIMAL:
		case Types.NUMERIC:
			oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, BigDecimalSerializer.get());
			break;

		default:
			oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, s);
		}

		oQry.setColumnFamily(sName);
		oQry.setColumnNames(new String[] {keys[0].getName()});
		oQry.setRowCount(1);

		oQry.setKeys(lastKeyForMissing, "");
		oRes = oQry.execute();
		oRows = (OrderedRows<String, String, ?>) oRes.get();
		return oRows.getCount()>0;
	}

	@Override
	public long count(String indexColumnName, Object valueSearched) throws JDOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched)
			throws JDOException {
		return fetch (fetchGroup, indexColumnName, valueSearched, Integer.MAX_VALUE, 0);
	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched,
												 int maxrows, int offset) throws JDOException {
		return fetch(fetchGroup, indexColumnName, (Comparable<?>) valueSearched, (Comparable<?>) valueSearched, maxrows, offset);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName,
												Comparable<?> valueFrom, Comparable<?> valueTo,
												int maxrows, int offset)
		throws JDOException, IllegalArgumentException {

		int skip = 0;
		RecordSet<R> oRetVal;
		try {
			oRetVal = (RecordSet<R>) newRecordSetOf(getResultClass(), DEFAULT_INITIAL_RECORDSET_SIZE);
		} catch (NoSuchMethodException e) {
			throw new JDOException(e.getMessage(), e);
		}

		switch (getColumnByName(indexColumnName).getType()) {

		case Types.BIGINT:
			RangeSlicesQuery<String, String, Long> oLQry = createQueryForLongRange(fetchGroup, indexColumnName, valueFrom, valueTo, maxrows, offset);
			for (Row<String, String, Long> oRow : oLQry.execute().get())
				if (skip++<offset)
					oRetVal.add((R) convertHectorRowToRecordLng(newStandardRow(), oRow));
			break;

		case Types.INTEGER:
			RangeSlicesQuery<String, String, Integer> oIQry = createQueryForIntegerRange(fetchGroup, indexColumnName, valueFrom, valueTo, maxrows, offset);
			for (Row<String, String, Integer> oRow : oIQry.execute().get())
				if (skip++<offset)
					oRetVal.add((R) convertHectorRowToRecordInt(newStandardRow(), oRow));
			break;

		case Types.DOUBLE:
			RangeSlicesQuery<String, String, Double> oDQry = createQueryForDoubleRange(fetchGroup, indexColumnName, valueFrom, valueTo, maxrows, offset);
			for (Row<String, String, Double> oRow : oDQry.execute().get())
				if (skip++<offset)
					oRetVal.add((R) convertHectorRowToRecordDbl(newStandardRow(), oRow));
			break;

		case Types.FLOAT:
			RangeSlicesQuery<String, String, Float> oFQry = createQueryForFloatRange(fetchGroup, indexColumnName, valueFrom, valueTo, maxrows, offset);
			for (Row<String, String, Float> oRow : oFQry.execute().get())
				if (skip++<offset)
					oRetVal.add((R) convertHectorRowToRecordFlt(newStandardRow(), oRow));
			break;

		case Types.DECIMAL:
		case Types.NUMERIC:
			RangeSlicesQuery<String, String, BigDecimal> oBQry = createQueryForBigDecimalRange(fetchGroup, indexColumnName, valueFrom, valueTo, maxrows, offset);
			for (Row<String, String, BigDecimal> oRow : oBQry.execute().get())
				if (skip++<offset)
					oRetVal.add((R) convertHectorRowToRecordDec(newStandardRow(), oRow));
			break;

		default:
			RangeSlicesQuery<String, String, String> oSQry = createQueryForStringRange(fetchGroup, indexColumnName, valueFrom, valueTo, maxrows, offset);
			for (Row<String, String, String> oRow : oSQry.execute().get())
				if (skip++<offset)
					oRetVal.add((R) convertHectorRowToRecordStr(newStandardRow(), oRow));
		}
		return oRetVal;

	}

	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Comparable<?> valueFrom,
			Comparable<?> valueTo) throws JDOException, IllegalArgumentException {
		return fetch(fetchGroup, indexColumnName, valueFrom, valueTo, Integer.MAX_VALUE, 0);
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

	@SuppressWarnings("unchecked")
	private Record newStandardRow() {
		Object[] constructorParameters;
		if (null==oRecConstructor) {
			oRecConstructor = (Constructor<? extends Record>) getConstructor(getResultClass(), new Class<?>[]{TableDef.class});
		}
		try {
			constructorParameters = filterParameters(oRecConstructor.getParameters(), new Object[]{oKeySpace.getMetaData().getTable(name())});
		} catch (InstantiationException e) {
			throw new JDOException(e.getMessage() + " getting constructor for MongoDocument subclass");
		}
		return newRecord(oRecConstructor, constructorParameters);
	}

	private RangeSlicesQuery<String, String, Long> createQueryForLongRange(FetchGroup fetchGroup,
			String indexColumnName, Comparable<?> valueFrom, Comparable<?> valueTo, int maxrows, int offset) {
		StringSerializer s = StringSerializer.get();
		RangeSlicesQuery<String, String, Long> oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, LongSerializer.get())
		.setColumnFamily(sName)
		.setColumnNames(getColumNamesArray(fetchGroup));
		setMaxRows(oQry, maxrows, offset);
		if (valueFrom!=null && valueFrom.equals(valueTo)) {
			oQry.addEqualsExpression(indexColumnName, (Long) (valueFrom instanceof Long ? valueFrom : new Long(valueFrom.toString())));
		} else {
			if (valueFrom!=null)
				oQry.addGteExpression(indexColumnName, (Long) (valueFrom instanceof Long ? valueFrom : new Long(valueFrom.toString())));
			if (valueTo!=null)
				oQry.addLteExpression(indexColumnName, (Long) (valueTo instanceof Long ? valueTo : new Long(valueTo.toString())));
		}
		return oQry;
	}

	private RangeSlicesQuery<String, String, Integer> createQueryForIntegerRange(FetchGroup fetchGroup,
			String indexColumnName, Comparable<?> valueFrom, Comparable<?> valueTo, int maxrows, int offset) {
		StringSerializer s = StringSerializer.get();
		RangeSlicesQuery<String, String, Integer> oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, IntegerSerializer.get())
		.setColumnFamily(sName)
		.setColumnNames(getColumNamesArray(fetchGroup));
		setMaxRows(oQry, maxrows, offset);
		if (valueFrom!=null && valueFrom.equals(valueTo)) {
			oQry.addEqualsExpression(indexColumnName, (Integer) (valueFrom instanceof Long ? valueFrom : new Integer(valueFrom.toString())));
		} else {
			if (valueFrom!=null)
				oQry.addGteExpression(indexColumnName, (Integer) (valueFrom instanceof Long ? valueFrom : new Integer(valueFrom.toString())));
			if (valueTo!=null)
				oQry.addLteExpression(indexColumnName, (Integer) (valueTo instanceof Long ? valueTo : new Integer(valueTo.toString())));
		}
		return oQry;
	}

	private RangeSlicesQuery<String, String, Double> createQueryForDoubleRange(FetchGroup fetchGroup,
			String indexColumnName, Comparable<?> valueFrom, Comparable<?> valueTo, int maxrows, int offset) {
		StringSerializer s = StringSerializer.get();
		RangeSlicesQuery<String, String, Double> oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, DoubleSerializer.get())
		.setColumnFamily(sName)
		.setColumnNames(getColumNamesArray(fetchGroup));
		setMaxRows(oQry, maxrows, offset);
		if (valueFrom!=null && valueFrom.equals(valueTo)) {
			oQry.addEqualsExpression(indexColumnName, (Double) (valueFrom instanceof Long ? valueFrom : new Double(valueFrom.toString())));
		} else {
			if (valueFrom!=null)
				oQry.addGteExpression(indexColumnName, (Double) (valueFrom instanceof Long ? valueFrom : new Double(valueFrom.toString())));
			if (valueTo!=null)
				oQry.addLteExpression(indexColumnName, (Double) (valueTo instanceof Long ? valueTo : new Double(valueTo.toString())));
		}
		return oQry;
	}

	private RangeSlicesQuery<String, String, Float> createQueryForFloatRange(FetchGroup fetchGroup,
			String indexColumnName, Comparable<?> valueFrom, Comparable<?> valueTo, int maxrows, int offset) {
		StringSerializer s = StringSerializer.get();
		RangeSlicesQuery<String, String, Float> oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, FloatSerializer.get())
		.setColumnFamily(sName)
		.setColumnNames(getColumNamesArray(fetchGroup));
		setMaxRows(oQry, maxrows, offset);
		if (valueFrom!=null && valueFrom.equals(valueTo)) {
			oQry.addEqualsExpression(indexColumnName, (Float) (valueFrom instanceof Long ? valueFrom : new Float(valueFrom.toString())));
		} else {
			if (valueFrom!=null)
				oQry.addGteExpression(indexColumnName, (Float) (valueFrom instanceof Long ? valueFrom : new Float(valueFrom.toString())));
			if (valueTo!=null)
				oQry.addLteExpression(indexColumnName, (Float) (valueTo instanceof Long ? valueTo : new Float(valueTo.toString())));
		}
		return oQry;
	}

	private RangeSlicesQuery<String, String, BigDecimal> createQueryForBigDecimalRange(FetchGroup fetchGroup,
			String indexColumnName, Comparable<?> valueFrom, Comparable<?> valueTo, int maxrows, int offset) {
		StringSerializer s = StringSerializer.get();
		RangeSlicesQuery<String, String, BigDecimal> oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, BigDecimalSerializer.get())
		.setColumnFamily(sName)
		.setColumnNames(getColumNamesArray(fetchGroup));
		setMaxRows(oQry, maxrows, offset);
		if (valueFrom!=null && valueFrom.equals(valueTo)) {
			oQry.addEqualsExpression(indexColumnName, (BigDecimal) (valueFrom instanceof Long ? valueFrom : new BigDecimal(valueFrom.toString())));
		} else {
			if (valueFrom!=null)
				oQry.addGteExpression(indexColumnName, (BigDecimal) (valueFrom instanceof Long ? valueFrom : new BigDecimal(valueFrom.toString())));
			if (valueTo!=null)
				oQry.addLteExpression(indexColumnName, (BigDecimal) (valueTo instanceof Long ? valueTo : new BigDecimal(valueTo.toString())));
		}
		return oQry;
	}

	private RangeSlicesQuery<String, String, String> createQueryForStringRange(FetchGroup fetchGroup,
			String indexColumnName, Comparable<?> valueFrom, Comparable<?> valueTo, int maxrows, int offset) {
		StringSerializer s = StringSerializer.get();
		RangeSlicesQuery<String, String, String> oQry = createRangeSlicesQuery(oKeySpace.keySpace(), s, s, s)
		.setColumnFamily(sName)
		.setColumnNames(getColumNamesArray(fetchGroup));
		setMaxRows(oQry, maxrows, offset);
		if (valueFrom!=null && valueFrom.equals(valueTo)) {
			oQry.addEqualsExpression(indexColumnName, valueFrom.toString());
		} else {
			if (valueFrom!=null)
				oQry.addGteExpression(indexColumnName, valueFrom.toString());
			if (valueTo!=null)
				oQry.addLteExpression(indexColumnName, valueTo.toString());
		}
		return oQry;
	}

	private void setMaxRows(RangeSlicesQuery<String, String, ?> oQry, int maxrows, int offset) {
		if (maxrows>0) {
			try {
				oQry.setRowCount(maxrows>=0 ? Math.addExact(maxrows, offset) : Integer.MAX_VALUE);
			} catch (ArithmeticException e) {
				oQry.setRowCount(Integer.MAX_VALUE);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private String[] getColumNamesArray(FetchGroup fetchGroup) {
		String[] oColNames = new String[fetchGroup.getMembers().size()];
		int c = 0;
		Iterator<String> oColIter = fetchGroup.getMembers().iterator();
		while (oColIter.hasNext())
			oColNames[c++] = oColIter.next();
		return oColNames;
	}

}
