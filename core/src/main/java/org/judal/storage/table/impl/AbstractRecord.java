package org.judal.storage.table.impl;

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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;
import javax.jdo.JDOUserException;
import javax.jdo.metadata.ColumnMetadata;

import org.judal.metadata.ViewDef;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.PrimaryKeyDef;
import org.judal.metadata.SelectableDef;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.FieldHelper;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.Table;
import org.judal.storage.table.TableDataSource;

import com.knowgate.debug.DebugFile;
import com.knowgate.gis.LatLong;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;

import java.text.ParseException;

/**
 * <p>Partial implementation of Record interface.</p>
 * <p>The actual storage of rows is determined by a subclass.
 * It may be storing each row as Java member variables, an array or as a map
 * depending on whether PojoRecord, ArrayRecord, MapRecord is used.</p>
 * <p>Each AbstractRecord is associated with an underlying ViewDef or TableDef
 * which describes the list of columns held by the Record.
 * The ViewDef or TableDef may represent an actual table or view in the database
 * or may be generated on the fly to represent any set of columns.</p>
 * <p>If the actual implementation is of type MapRecord, then the Record may hold
 * more columns than just the ones described in the ViewDef/TableDef. This is in
 * order to accommodate varying columns list from schemaless storage systems or
 * growing column families from column-oriented databases.</p>
 * <p>It is possible to associate a ConstraintsChecker and a FieldHelper to the Record.
 * These classes help to perform client-side validations on data integrity and custom
 * type conversion for non-standard types of a database.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 *
 */
public abstract class AbstractRecord extends AbstractRecordBase {

	private static final long serialVersionUID = 10000l;

	protected String tableName;
	protected transient SelectableDef tableDef;
	protected transient ConstraintsChecker checker;
	protected transient FieldHelper fhelper;
	protected boolean hasLongVarBinaryData;
	protected HashMap<String,Long> longVarBinariesLengths;

	/**
	 * <p>Construct AbstractRecord reading the metadata from a ViewDef or TableDef and with no ConstraintsChecker nor FieldHelper.</p>
	 * @param tableDefinition ViewDef or TableDef
	 * @throws JDOUserException if tableDefinition is <b>null</b>
	 */
	public AbstractRecord(ViewDef tableDefinition) throws JDOUserException {
		this(tableDefinition, null, null);
	}

	/**
	 * <p>Construct AbstractRecord reading the metadata from a ViewDef or TableDef and with the given ConstraintsChecker and no FieldHelper.</p>
	 * @param tableDefinition ViewDef or TableDef
	 * @param constraintsChecker ConstraintsChecker
	 * @throws JDOUserException if tableDefinition is <b>null</b>
	 */
	public AbstractRecord(ViewDef tableDefinition, ConstraintsChecker constraintsChecker) throws JDOUserException {
		this(tableDefinition, null, constraintsChecker);
	}

	/**
	 * <p>Construct AbstractRecord reading the metadata from a ViewDef or TableDef and with the given FieldHelper and no ConstraintsChecker.</p>
	 * @param tableDefinition ViewDef or TableDef
	 * @param fieldHelper FieldHelper
	 * @throws JDOUserException if tableDefinition is <b>null</b>
	 */
	public AbstractRecord(ViewDef tableDefinition, FieldHelper fieldHelper) throws JDOUserException {
		this(tableDefinition, fieldHelper, null);
	}

	/**
	 * <p>Construct AbstractRecord reading the metadata directly from a DataSource capable of providing it.</p>
	 * @param dataSource TableDataSource
	 * @param tableName Name of table or view
	 * @throws JDOException
	 * @throws JDOUserException if dataSource is <b>null</b>
	 */
	public AbstractRecord(TableDataSource dataSource, String tableName) throws JDOException {
		this(dataSource, tableName, dataSource.getFieldHelper(), null);
	}

	/**
	 * <p>Construct AbstractRecord reading the metadata directly from a DataSource capable of providing it.</p>
	 * @param dataSource TableDataSource
	 * @param tableName Name of table or view
	 * @param fieldHelper FieldHelper
	 * @throws JDOException
	 * @throws JDOUserException if dataSource is <b>null</b>
	 */
	public AbstractRecord(TableDataSource dataSource, String tableName, FieldHelper fieldHelper) throws JDOException {
		this(dataSource, tableName, fieldHelper, null);
	}
	
	/**
	 * <p>Construct AbstractRecord reading the metadata directly from a DataSource capable of providing it.</p>
	 * @param dataSource TableDataSource
	 * @param tableName Name of table or view
	 * @param constraintsChecker ConstraintsChecker
	 * @throws JDOException
	 * @throws JDOUserException if dataSource is <b>null</b>
	 */
	public AbstractRecord(TableDataSource dataSource, String tableName, ConstraintsChecker constraintsChecker) throws JDOException {
		this(dataSource, tableName, dataSource.getFieldHelper(), constraintsChecker);
	}

	/**
	 * <p>Construct AbstractRecord reading the metadata from a ViewDef or TableDef and with the given FieldHelper and ConstraintsChecker.</p>
	 * @param tableDefinition ViewDef or TableDef
	 * @param fieldHelper FieldHelper
	 * @param constraintsChecker ConstraintsChecker
	 * @throws JDOUserException if tableDefinition is <b>null</b>
	 */
	public AbstractRecord(ViewDef tableDefinition, FieldHelper fieldHelper, ConstraintsChecker constraintsChecker) throws JDOUserException {
		if (null==tableDefinition)
			throw new JDOUserException("AbstractRecord constructor. ViewDef cannot be null");
		tableDef = tableDefinition;
		tableName = tableDef.getName();
		setFieldHelper(fieldHelper);
		setConstraintsChecker(constraintsChecker);
		clearLongData();
	}

	/**
	 * <p>Construct AbstractRecord reading the metadata directly from a DataSource capable of providing it.</p>
	 * @param dataSource TableDataSource
	 * @param tableOrViewName Name of table or view
	 * @param fieldHelper FieldHelper
	 * @param constraintsChecker ConstraintsChecker
	 * @throws JDOException
	 * @throws JDOUserException if dataSource is <b>null</b>
	 */
	public AbstractRecord(TableDataSource dataSource, String tableOrViewName, FieldHelper fieldHelper, ConstraintsChecker constraintsChecker) throws JDOException {
		if (null==dataSource)
			throw new JDOUserException("No DataSource specified and no one provided by EngineFactory neither");
		tableDef = dataSource.getTableDef(tableOrViewName);
		if (null==tableDef)
			throw new JDOException("Table "+tableOrViewName+" does not exist or could not be read from the data source");
		tableName = tableDef.getName();
		setFieldHelper(fieldHelper);
		setConstraintsChecker(constraintsChecker);
		clearLongData();
	}
	
	/**
	 * <p>Get the value of a column.</p>
	 * @return Object Returned value may be <b>null</b> or an Option[AnyRef] if using implementation for Scala.
	 */
	@Override
	public abstract Object apply(String columnName);

	/**
	 * <p>Set value of a column.</p>
	 * @param colname String Column Name
	 * @param value Object Column value
	 */
	@Override
	public abstract Object put(String colname, Object value);

	/**
	 * <p>Remove value of a column.</p>
	 * This method may have different behaviours depending on the implementation.
	 * For MapRecord subclass, removing a column will cause that it won't be updated in the database.
	 * But for ArrayRecord and PojoRecord, removing a column will set it to <b>null</b> or cause an exception.
	 * @param colname String Column Name
	 */
	@Override
	public abstract Object remove(String colname);

	/**
	 * <p>Remove values for all columns.</p>
	 * This method may have different behaviours depending on the implementation.
	 */
	@Override
	public abstract void clear();

	/**
	 * @return ConstraintsChecker
	 */
	@Override
	public ConstraintsChecker getConstraintsChecker() {
		return checker;
	}

	/**
	 * @param checker ConstraintsChecker
	 */
	public void setConstraintsChecker(ConstraintsChecker checker) {
		this.checker = checker;
	}

	/**
	 * @return FieldHelper
	 */
	public FieldHelper getFieldHelper() {
		return fhelper;
	}

	/**
	 * @param helper FieldHelper
	 */
	public void setFieldHelper(FieldHelper helper) {
		this.fhelper = helper;
	}

	/**
	 * <p>Replace value of a column.</p>
	 * @param colname String Column Name
	 * @param newvalue Object New value for column
	 * @return Object Former value of column
	 */
	@Override
	public Object replace(String colname, Object newvalue) {
		Object retval = apply(colname);
		remove(colname);
		put(colname, newvalue);
		return retval;
	}
	
	/**
	 * <p>Load Record using given DataSource.</p>
	 * @param oDts TableDataSource
	 * @param oKey Object Value for primary key. May actually be Object[] if the key has multiple columns.
	 * @return <b>true</b> if a Record was found with the given primary key, <b>false</b>otherwise.
	 * @throws JDOException If the underlying table has no primary key
	 * @throws ClassCastException If oDts is not an instance of class TableDataSource or a subclass of it.
	 */
	@Override
	public boolean load(DataSource oDts, Object oKey) throws JDOException, ClassCastException {
		boolean bLoaded = false;
		try (Table oTbl = ((TableDataSource) oDts).openTable(this)) {
			bLoaded = oTbl.load(oKey, this);
		}
		return bLoaded;
	}

	/**
	 * <p>Load Record using EngineFactory.getDefaultTableDataSource().</p>
	 * @param oKey Object Value for primary key. May actually be Object[] if the key has multiple columns.
	 * @return <b>true</b> if a Record was found with the given primary key, <b>false</b>otherwise.
	 * @throws JDOException If the underlying table has no primary key
	 * @throws NullPointerException If EngineFactory.getDefaultTableDataSource() is not set
	 */
	@Override
	public boolean load(Object oKey) throws JDOException,NullPointerException {
		return load(EngineFactory.getDefaultTableDataSource(), oKey);
	}

	/**
	 * <p>Store this Record using given DataSource.</p>
	 * A store operation will insert the Record if it does not exist or update it if already exists.
	 * If a ConstraintsChecker has been set, its check() method will be called before attempting to store the Record.
	 * This may result in a JDOException thrown if the Record does not comply with the constraints.
	 * @param oDts TableDataSource
	 * @throws JDOException
	 * @throws ClassCastException If oDts is not an instance of class TableDataSource or a subclass of it.
	 */
	@Override
	public void store(DataSource oDts) throws JDOException,ClassCastException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin AbstractRecord.store(" + oDts.getClass().getName() + ") " + getClass().getName());
			DebugFile.incIdent();
		}
		if (getConstraintsChecker()!=null)
			getConstraintsChecker().check(oDts, this);
		try (Table oTbl = ((TableDataSource) oDts).openTable(this)) {
			oTbl.store(this);
		} finally {
			if (DebugFile.trace) DebugFile.decIdent();
		}
		if (DebugFile.trace)
			DebugFile.writeln("End AbstractRecord.store(" + oDts.getClass().getName() + ") : " +  getKey());
	}

	/**
	 * <p>Store this Record using EngineFactory.getDefaultTableDataSource().</p>
	 * A store operation will insert the Record if it does not exist or update it if already exists.
	 * If a ConstraintsChecker has been set, its check() method will be called before attempting to store the Record.
	 * This may result in a JDOException thrown if the Record does not comply with the constraints.
	 * @throws JDOException
	 */
	@Override
	public void store() throws JDOException {
		store(EngineFactory.getDefaultTableDataSource());
	}

	/**
	 * <p>Delete this Record using the given DataSource.</p>
	 * @param oDts TableDataSource
	 * @throws JDOException
	 * @throws ClassCastException If oDts is not an instance of class TableDataSource or a subclass of it.
	 */
	@Override
	public void delete(DataSource oDts) throws JDOException,ClassCastException {
		Table oTbl = ((TableDataSource) oDts).openTable(this);
		try {
			oTbl.delete(getKey());
		} finally {
			if (oTbl!=null) oTbl.close();
		}
	}

	/**
	 * <p>Delete this Record using EngineFactory.getDefaultTableDataSource().</p>
	 * @throws JDOException
	 */
	@Override
	public void delete() throws JDOException {
		delete(EngineFactory.getDefaultTableDataSource());
	}
	
	/**
	 * @return SelectableDef
	 */
	public SelectableDef getTableDef() {
		return tableDef;
	}

	/**
	 * @param selectableDef SelectableDef
	 */
	public void setTableDef(SelectableDef selectableDef) {
		tableDef = selectableDef;
		tableName = tableDef.getName();
	}
	
	public void clearLongData() {
		hasLongVarBinaryData = false;
		longVarBinariesLengths = null;
	}

	/**
	 * @return boolean <b>true</b> if this Record contains any LONGVARCHAR, LONGVARBINARY, CLOB or BLOB column
	 */
	public boolean hasLongData() {
		return hasLongVarBinaryData;
	}

	/**
	 * @return Map&lt;String,Long&gt;
	 */
	public Map<String,Long> longDataLengths() {
		return longVarBinariesLengths;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ColumnDef[] columns() {
		return tableDef.getColumns();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FetchGroup fetchGroup() {
		FetchGroup group = new ColumnGroup();
		for (ColumnDef cdef : columns())
			group.addMember(cdef.getName());
		group.addCategory(FetchGroup.DEFAULT);
		return group;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ColumnDef getColumn(String colname) throws ArrayIndexOutOfBoundsException {
		return tableDef.getColumnByName(colname);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getBucketName() {
		return tableName;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTableName() {
		return tableName;
	}

	/**
	 * <p>Get value of primary key.</p>
	 * @return Value of record primary key. Will be an array of Object if the primary key is composed of multiple columns.
	 * @throws JDOUserException if table or view has no primary key
	 */
	@Override
	public Object getKey() throws JDOException {
		Object retval;
		PrimaryKeyDef pk = tableDef.getPrimaryKeyMetadata();
		if (null==pk)
			throw new JDOUserException("Table or view "+getTableName()+"  has no primary key");
		/*
		if (DebugFile.trace) {
			DebugFile.writeln("Begin AbstractRecord.getKey()");
			DebugFile.incIdent();
			DebugFile.writeln("primary key has " +  String.valueOf(pk.getNumberOfColumns()) + " columns");
		}
		*/

		if (pk.getNumberOfColumns()==0) {
			retval = null;
		} else if (pk.getNumberOfColumns()==1) {
			retval = apply(pk.getColumn());
			// if (DebugFile.trace) DebugFile.writeln(pk.getColumn() + "=" + retval);
			if (retval instanceof String) {
				try {
					retval = pk.getColumns()[0].convert((String) retval);
				} catch (NumberFormatException | NullPointerException | ParseException e) {
					throw new JDOUserException("AbstractRecord.getKey() " + e.getClass().getName()+" " + e.getMessage());
				}
			}
		}  else {
			Object[] retvals = new Object[pk.getNumberOfColumns()];
			int c = 0;
			for (ColumnMetadata col : pk.getColumns()) {
				retvals[c] = apply(col.getName());
				if (retvals[c] instanceof String) {
					try {
						retvals[c] = pk.getColumns()[c].convert((String) retvals[c]);
					} catch (NumberFormatException | NullPointerException | ParseException e) {
						throw new JDOUserException("AbstractRecord.getKey() " + e.getClass().getName()+" " + e.getMessage());
					}
				}
				c++;
			}
			retval = (Object) retvals;
		}

		/*
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End AbstractRecord.getKey() : " + retval + (retval!=null ? " of class " + retval.getClass().getName() : ""));
		}
		*/

		return (Object) retval;
	}

	/**
	 * <p>Set value of primary key for this record.</p>
	 * @param value If primary key is composed of multiple columns then must be an array of Object
	 * @throws JDOException
	 */
	@Override
	public void setKey(Object value) throws JDOException {
		PrimaryKeyDef pk = tableDef.getPrimaryKeyMetadata();
		if (pk.getNumberOfColumns()==0) throw new JDOException("Table "+tableDef.getName()+" has no primary key");
		if (pk.getNumberOfColumns()==1) {
			if (DebugFile.trace) DebugFile.writeln("set "+pk.getColumn()+"="+value);
			if (pk.getColumn()==null)
				throw new JDOUserException("Primary key column name for table "+tableDef.getName()+" is null");
			else if (pk.getColumn().trim().length()==0)
				throw new JDOUserException("Primary key column name for table "+tableDef.getName()+" is empty");
			if (value instanceof String) {
				try {
					put(pk.getColumn(), pk.getColumns()[0].convert((String) value));
				} catch (NumberFormatException | NullPointerException | ParseException e) {
					throw new JDOUserException("AbstractRecord.setKey("+ value + ") "+e.getClass().getName()+" "+e.getMessage());
				}
			} else {
				put(pk.getColumn(), value);
			}
		} else {
			try {
				Object[] vals = (Object[]) value;
				int c = 0;
				for (ColumnMetadata col : pk.getColumns()) {
					if (value instanceof String) {
						try {
							put(col.getName(), pk.getColumns()[c].convert((String) vals[c]));
							c++;
						} catch (NumberFormatException | NullPointerException | ParseException e) {
							throw new JDOUserException("AbstractRecord.setKey("+ value + ") "+e.getClass().getName()+" "+e.getMessage());
						}
					}  else {
						put(col.getName(), vals[c++]);
					}
				}
			} catch (ClassCastException cce) {
				String pkcols = "";
				for (ColumnMetadata col : pk.getColumns())
					pkcols += "," + col.getName();
				throw new JDOException("AbstractRecord.setKey() Key value bust be either a basic type or and Object[] but was "+value.getClass().getName()+" for ("+pkcols.substring(1)+")", cce);
			}
		}
	}

	/**
	 * <p>Set value of content, contentType and contentLength fields.</p>
	 * @param bytes byte[] Value of content field
	 * @param contentType String Value of contentType field
	 * @throws JDOException
	 */
	@Override
	public void setContent(byte[] bytes, String contentType) throws JDOException {
		put("content", bytes);
		put("contentType", contentType);
		if (isNullOrNone(bytes))
			put("contentLength", new Long(0l));
		else
			put("contentLength", new Long(bytes.length));			
	}

	/**
	 * <p>Get value of an interval part.</p>
	 * @param sColName String Column Name
	 * @param sPart String Part Identifier
	 * @return int
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 * @throws NullPointerException
	 * @throws NumberFormatException
	 * @throws IllegalArgumentException
	 */
	@Override
	public int getIntervalPart(String sColName, String sPart) throws ClassCastException, ClassNotFoundException, NullPointerException, NumberFormatException, IllegalArgumentException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getIntervalPart(this, sColName, sPart);
	}

	/**
	 * @param sColName String Column Name
	 * @return LatLong
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 * @throws NumberFormatException
	 * @throws ArrayIndexOutOfBoundsException
	 */
	@Override
	public LatLong getLatLong(String sColName) throws ClassCastException, ClassNotFoundException, NumberFormatException, ArrayIndexOutOfBoundsException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		if (isNull(sColName))
			return null;
		else
			return fhelper.getLatLong(this, sColName);
	}

	/**
	 * <p>Get Integer array value.</p>
	 * @param sColName String
	 * @return Integer[]
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 */
	@Override
	public Integer[] getIntegerArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getIntegerArray(this, sColName);
	}

	/**
	 * <p>Get Long array value.</p>
	 * @param sColName String
	 * @return Long[]
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 */
	@Override
	public Long[] getLongArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getLongArray(this, sColName);
	}

	/**
	 * <p>Get Float array value.</p>
	 * @param sColName String
	 * @return Float[]
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 */
	@Override
	public Float[] getFloatArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getFloatArray(this, sColName);
	}

	/**
	 * <p>Get Double array value.</p>
	 * @param sColName String
	 * @return Double[]
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 */
	@Override
	public Double[] getDoubleArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getDoubleArray(this, sColName);
	}

	/**
	 * <p>Get Date array value.</p>
	 * @param sColName String
	 * @return Date[]
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 */
	@Override
	public Date[] getDateArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getDateArray(this, sColName);
	}
	
	/**
	 * <p>Get String array value.</p>
	 * @param sColName String
	 * @return String[]
	 * @throws ClassNotFoundException If no FieldHelper has been set for this Record
	 * @throws ClassCastException
	 */
	@Override
	public String[] getStringArray(String sColName) throws ClassCastException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getStringArray(this, sColName);
	}
	
	/**
	 * <p>Get value of column holding a map of key-value pairs.</p>
	 * @param sColName String Column Name
	 * @return Object A Map which class depends on the FieldHelper implementation
	 * @throws ClassNotFoundException if not FieldHelper has been set
	 * @throws ClassCastException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 */
	@Override
	public Object getMap(String sColName) throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		if (null==fhelper)
			throw new ClassNotFoundException("No FieldHelper class has been specified for " + getTableName());
		return fhelper.getMap(this, sColName);
	}
	
	/**
	 * @return <b>true</b> if Record does not contain the given key or its value is <b>null</b> or its value is an empty string ""
	 */
	@Override
	public boolean isEmpty(String sColName) {
		Object obj = apply(sColName);
		if (isNullOrNone(obj))
			return true;
		else if (obj instanceof String)
			return ((String) obj).length()==0;
		else
			return false;
	}

	/**
	 * <p>Test is a field is <b>null</b> or <b>None</b>.</p>
	 * If the sColName is a String and its value is "null" Then it will also be considered <b>null</b>
	 * @param sColName Column Name
	 * @return <b>true</b> if field is null or None or if it is a String value "null" or if no column with given name was found 
	 */
	@Override  
	public boolean isNull(String sColName) {
		Object obj = apply(sColName);
		if (isNullOrNone(obj))
			return true;
		else if (obj instanceof String)
			return ((String) obj).equalsIgnoreCase("null");
		else
			return false;
	}


	protected Object getBinaryData(String sKey, Object oObj) {
		Object retObj = null;
		if (oObj instanceof byte[]) {
			if (!hasLongVarBinaryData) longVarBinariesLengths = new HashMap<String,Long>();	
			longVarBinariesLengths.put(sKey, new Long(((byte[])oObj).length));
			hasLongVarBinaryData = true;
			retObj = oObj;
		} else {
			Class[] aInts = oObj.getClass().getInterfaces();
			if (null==aInts) {
				retObj = oObj;
			} else {
				boolean bIsSerializable = false;
				for (int i=0; i<aInts.length && !bIsSerializable; i++)
					bIsSerializable |= aInts[i].getName().equals("java.io.Serializable");
				if (bIsSerializable) {
					try {
						ByteArrayOutputStream oBOut = new ByteArrayOutputStream();
						ObjectOutputStream oOOut = new ObjectOutputStream(oBOut);
						oOOut.writeObject(oObj);
						byte[] aBytes = oBOut.toByteArray();
						if (aBytes!=null) {
							if (!hasLongVarBinaryData) longVarBinariesLengths = new HashMap<String,Long>();	
							longVarBinariesLengths.put(sKey, new Long(aBytes.length));
							hasLongVarBinaryData = true;
							retObj = aBytes;
						}
						oOOut.close();
						oBOut.close();              
					} catch (IOException neverthrown) { }
				} else {
					retObj = oObj;
				}
			}
		}
		return retObj;
	}

	public static String getColumnAlias(String columnName) {
		final int len = columnName.length()-5;
		for (int n=0; n<len; n++)
			if (columnName.charAt(n)==' ' &&
			   (columnName.charAt(n+1)=='A' || columnName.charAt(n+1)=='a') &&
			   (columnName.charAt(n+2)=='S' || columnName.charAt(n+2)=='s') &&
			    columnName.charAt(n+3)==' ')
			  return columnName.substring(n+4);
		return columnName;
	}
	
}
