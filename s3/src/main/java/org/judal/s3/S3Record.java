package org.judal.s3;

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
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.sql.Types;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.knowgate.gis.LatLong;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;
import org.judal.serialization.BytesConverter;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.FieldHelper;
import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.impl.AbstractRecordBase;

/**
 * <p>Record implementation for Amazon S3 objects.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class S3Record extends AbstractRecordBase {

	private static final long serialVersionUID = 1L;
	
	private static final String content = "content";
	private static final String contentLength = "contentLength";

	public final String[] standardProperties = new String[]{"cachecontrol",content,"contentdisposition","contentencoding","contentlength","contentmd5","contenttype","etag","expirationtime","expirationtimeRuleId","httpexpiresdate","lastmodified","restoreexpirationtime","serversideencryption","versionid"};

	private String key;

	private HashMap<String,Object> metadataProperties;
	
	private TableDef tdef;
	
	public S3Record(TableDef tableDef) throws JDOException {
		tdef = tableDef;
		metadataProperties = new HashMap<String,Object>();
	}

	public S3Record(String bucketName) throws JDOException {
		this(new S3TableDef(bucketName));
		metadataProperties = new HashMap<String,Object>();
		for (String columnName : standardProperties)
			tdef.addColumnMetadata("", columnName, Types.VARCHAR, true);
	}

	/**
	 * Constructor
	 * @param bucketName String
	 * @param columnNames String[] Metadata column names
	 * @throws JDOException
	 */
	public S3Record(String bucketName, String... columnNames) throws JDOException {
		this(new S3TableDef(bucketName));
		for (String columnName : columnNames)
			tdef.addColumnMetadata("", columnName, Types.VARCHAR, true);
	}

	@Override
	public String getKey() {
		return key;
	}

	/**
	 * @param value String
	 * @throws NullPointerException If value is <b>null</b>
	 * @throws ClassCastException If value cannot be casted to String
	 */
	@Override
	public void setKey(Object value) throws NullPointerException {
		if (null==value) throw new NullPointerException("S3 object key cannot be null");
		key = (String) value; 
	}

	/**
	 * @return byte[]
	 */
	@Override
	public byte[] getValue() {
		return getBytes(content);
	}

	/**
	 * @param value Serializable If value instanceof byte[] then it will be set directly, else BytesConverter.toBytes(value, Types.JAVA_OBJECT) will be called for converting the input into byte[]
	 */
	@Override
	public void setValue(Serializable value) {
		if (value instanceof byte[])
			setContent((byte[]) value, null);
		else
			setContent(BytesConverter.toBytes(value, Types.JAVA_OBJECT), null);
	}

	@Override
	public void setContent(byte[] bytes, String contentType) {
		if (bytes==null) {
			put(content, new byte[0]);
			put(contentLength, new Long(0l));
		} else {
			put(content, bytes);
			put(contentLength, new Long(bytes.length));
		}
		if (contentType!=null)
			put("contentType", contentType);
		else
			remove("contentType");			
	}


	/**
	 * Set the value of this object content or one of its metadata properties
	 * @param columnName String If columnName is "content" then the value of this object will be assigned to <i>bytes</i> and the metadata property contentLength will be set to the length of <i>bytes</i>. If <i>columnName</i> is any other string then a metadata property with that name will be set to <i>bytes</i>.
	 * @param bytes byte[]
	 * @return byte[]
	 */
	@Override
	public byte[] put(String columnName, byte[] bytes) {
		Object retval = metadataProperties.get(columnName);
		if (columnName.equalsIgnoreCase(content)) {
			if (bytes!=null) {
				metadataProperties.put(content, bytes);
				metadataProperties.put(contentLength, new Long(bytes.length));
			} else {
				metadataProperties.put(content, new byte[0]);
				metadataProperties.put(contentLength, new Long(0l));
			}
		} else {
			if (bytes!=null)
				metadataProperties.put(columnName, bytes);
			else
				metadataProperties.remove(columnName);
		}
		if (retval==null)
			return null;
		else if (retval instanceof byte[])
			return (byte[]) retval;
		else
			return BytesConverter.toBytes(retval, Types.JAVA_OBJECT);
	}

	/**
	 * Set the value of this object content or one of its metadata properties
	 * @param columnName String If columnName is "content" then the value of this object will be assigned to BytesConverter.toBytes(<i>bytes</i>, Types.JAVA_OBJECT) and the metadata property contentLength will be set to the length of <i>bytes</i>. If <i>columnName</i> is any other string then a metadata property with that name will be set to <i>bytes</i>.
	 * @param value Object
	 * @return Object
	 */
	@Override
	public Object put(String columnName, Object value) {
		Object retval = metadataProperties.get(columnName);
		byte[] bytes;
		if (value!=null) {
			if (columnName.equalsIgnoreCase(content)) {
				if (value instanceof byte[]) {
					bytes = (byte[]) value;
				} else if (value instanceof String) {
					try {
						bytes = ((String) value).getBytes("UTF8");
					} catch (UnsupportedEncodingException neverthrow) { bytes = null; }
				} else {
					bytes = BytesConverter.toBytes(retval, Types.JAVA_OBJECT);
				}
				metadataProperties.put(content, bytes);
				metadataProperties.put(contentLength, new Long(bytes.length));
			} else {
				metadataProperties.put(columnName, value);
			}
		} else {
			metadataProperties.remove(columnName);
			if (columnName.equalsIgnoreCase(content))
				metadataProperties.put(contentLength, new Long(0l));  			
		}
		return retval;
	}

	public ObjectMetadata getMetadata() {
		ObjectMetadata oMDat = new ObjectMetadata();
		try {
			if (!isNull(content)) {
				MessageDigest oMsgd = MessageDigest.getInstance("MD5");
				Object oCnt = metadataProperties.get(content);
				if (oCnt instanceof byte[]) {
					oMDat.setContentMD5(Base64.getEncoder().encodeToString(oMsgd.digest((byte[]) oCnt)));
				} else if (oCnt instanceof String) {
					try {
						oMDat.setContentMD5(Base64.getEncoder().encodeToString(oMsgd.digest(((String)oCnt).getBytes("UTF8"))));
					} catch (UnsupportedEncodingException neverthrown) { }
				} else {
					oMDat.setContentMD5(null);
				}
			} else
				oMDat.setContentMD5(null);
		} catch (NoSuchAlgorithmException neverthrown) { }
		if (!isNull("cacheControl")) oMDat.setCacheControl(getString("cacheControl"));
		if (!isNull("contentDisposition")) oMDat.setContentDisposition(getString("contentDisposition"));
		if (!isNull("contentEncoding")) oMDat.setContentEncoding(getString("contentEncoding"));
		if (!isNull("contentLength")) oMDat.setContentLength(getLong("contentLength"));
		if (!isNull("contentType")) oMDat.setContentType(getString("contentType"));
		if (!isNull("expirationTime")) oMDat.setExpirationTime(getDate("expirationTime"));
		if (!isNull("expirationTimeRuleId")) oMDat.setExpirationTimeRuleId(getString("expirationTimeRuleId"));
		if (!isNull("httpExpiresDate")) oMDat.setHttpExpiresDate(getDate("httpExpiresDate"));
		if (!isNull("lastModified")) oMDat.setLastModified(getDate("lastModified"));
		if (!isNull("restoreExpirationTime")) oMDat.setRestoreExpirationTime(getDate("restoreExpirationTime"));
		if (!isNull("serverSideEncryption")) oMDat.setServerSideEncryption(getString("serverSideEncryption"));
		for (ColumnDef oCol : columns()) {
			String sColName = oCol.getName();
			if (!isNull(sColName) && Arrays.binarySearch(standardProperties, sColName.toLowerCase())<0) {
				oMDat.addUserMetadata(sColName, metadataProperties.get(sColName).toString());
			}
		}
		return oMDat;
	}

	@Override
	public String getBucketName() {
		return tdef.getTable();
	}

	/**
	 * <p>Load S3Record using EngineFactory.getDefaultTableDataSource().</p>
	 * @param key Object Value for primary key. May actually be Object[] if the key has multiple columns.
	 * @return <b>true</b> if a Record was found with the given primary key, <b>false</b>otherwise.
	 * @throws JDOException If the underlying table has no primary key
	 * @throws NullPointerException If EngineFactory.getDefaultTableDataSource() is not set
	 */
	@Override
	public boolean load(Object key) throws JDOException, NullPointerException {
		return load(EngineFactory.getDefaultTableDataSource(), key);
	}

	/**
	 * <p>Load S3Record using given S3DataSource.</p>
	 * @param dataSource S3DataSource
	 * @param key Object Value for primary key. May actually be Object[] if the key has multiple columns.
	 * @return <b>true</b> if a Record was found with the given primary key, <b>false</b>otherwise.
	 * @throws JDOException If the underlying table has no primary key
	 * @throws ClassCastException If oDts is not an instance of class S3DataSource.
	 */
	@Override
	public boolean load(DataSource dataSource, Object key) throws JDOException,ClassCastException {
		boolean bLoaded = false;
		try (Bucket oTbl = ((S3DataSource) dataSource).openBucket(tdef.getTable())) {
			bLoaded = oTbl.load(key, this);
		}
		return bLoaded;
	}

	/**
	 * <p>Store this S3Record using EngineFactory.getDefaultTableDataSource().</p>
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
	 * <p>Store this S3Record using given S3DataSource.</p>
	 * A store operation will insert the Record if it does not exist or update it if already exists.
	 * If a ConstraintsChecker has been set, its check() method will be called before attempting to store the Record.
	 * This may result in a JDOException thrown if the Record does not comply with the constraints.
	 * @param dataSource S3DataSource
	 * @throws JDOException
	 * @throws ClassCastException If dataSource is not an instance of class S3DataSource.
	 */	
	@Override
	public void store(DataSource dataSource) throws JDOException,ClassCastException {
		if (getConstraintsChecker()!=null)
			getConstraintsChecker().check(dataSource, this);
		try (Bucket oTbl = ((S3DataSource) dataSource).openBucket(tdef.getName())) {
			oTbl.store(this);
		}
	}

	/**
	 * <p>Delete this S3Record using the given S3DataSource.</p>
	 * @param dataSource TableDataSource
	 * @throws JDOException
	 * @throws ClassCastException If dataSource is not an instance of class S3DataSource.
	 */	
	@Override
	public void delete(DataSource dataSource) throws JDOException {
		Bucket oTbl = ((S3DataSource) dataSource).openBucket(tdef.getTable());
		try {
			oTbl.delete(getKey());
		} finally {
			if (oTbl!=null) oTbl.close();
		}
	}

	/**
	 * <p>Delete this S3Record using EngineFactory.getDefaultTableDataSource().</p>
	 * @throws JDOException
	 */	
	@Override
	public void delete() throws JDOException {
		delete(EngineFactory.getDefaultTableDataSource());
	}

	/**
	 * <p>Get metadata properties definitions.</p>
	 * @return ColumnDef[]
	 */
	@Override
	public ColumnDef[] columns() {
		return tdef.getColumns();
	}

	/**
	 * <p>Get metadata property definition.</p>
	 * @return ColumnDef
	 */
	@Override
	public ColumnDef getColumn(String columnName) throws ArrayIndexOutOfBoundsException {
		return tdef.getColumnByName(columnName);
	}

	/**
	 * <p>Get metadata properties as java.util.Map.</p>
	 * @return java.util.Map&lt;String,Object&gt;
	 */
	@Override
	public Map<String, Object> asMap() {
		return metadataProperties;
	}

	/**
	 * <p>Get metadata properties as Entry array.</p>
	 * @return Entry&lt;String,Object&gt;[]
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Entry<String, Object>[] asEntries() {
		return metadataProperties.entrySet().toArray(new Entry[metadataProperties.size()]);
	}

	@Override
	public boolean isNull(String colname) {
		if (metadataProperties.containsKey(colname))
			return metadataProperties.get(colname)==null;
		else
			return true;
	}

	@Override
	public boolean isEmpty(String colname) {
		if (metadataProperties.containsKey(colname))
			if (metadataProperties.get(colname) instanceof String)
				return metadataProperties.get(colname)==null || ((String) metadataProperties.get(colname)).length()==0;
			else
				return metadataProperties.get(colname)==null;
		else
			return true;
	}

	/**
	 * <p>Clear metadata properties.</p>
	 */
	@Override
	public void clear() {
		metadataProperties.clear();		
	}

	@Override
	public FieldHelper getFieldHelper() {
		return null;
	}

	@Override
	public String getTableName() {
		return tdef.getTable();
	}

	@Override
	public FetchGroup fetchGroup() {
		return new ColumnGroup(tdef.getColumnsStr().split(","));
	}

	@Override
	public ConstraintsChecker getConstraintsChecker() {
		return null;
	}

	@Override
	public Object apply(String colname) {
		return metadataProperties.get(colname);
	}

	@Override
	public int getIntervalPart(String colname, String part) throws ClassCastException, ClassNotFoundException,
			NullPointerException, NumberFormatException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Integer[] getIntegerArray(String colname) throws ClassCastException, ClassNotFoundException {
		return (Integer[]) metadataProperties.get(colname);
	}

	@Override
	public Long[] getLongArray(String colname) throws ClassCastException, ClassNotFoundException {
		return (Long[]) metadataProperties.get(colname);
	}

	@Override
	public Float[] getFloatArray(String colname) throws ClassCastException, ClassNotFoundException {
		return (Float[]) metadataProperties.get(colname);
	}

	@Override
	public Double[] getDoubleArray(String colname) throws ClassCastException, ClassNotFoundException {
		return (Double[]) metadataProperties.get(colname);
	}

	@Override
	public Date[] getDateArray(String colname) throws ClassCastException, ClassNotFoundException {
		return (Date[]) metadataProperties.get(colname);
	}

	@Override
	public LatLong getLatLong(String colname)
			throws ClassCastException, NumberFormatException, ArrayIndexOutOfBoundsException, ClassNotFoundException {
		return (LatLong) metadataProperties.get(colname);
	}

	@Override
	public String[] getStringArray(String colname) throws ClassCastException, ClassNotFoundException {
		return (String[]) metadataProperties.get(colname);
	}

	@Override
	public Object getMap(String colname)
			throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		return metadataProperties.get(colname);
	}

	@Override
	public Object put(int colpos, Object obj) throws IllegalArgumentException, ArrayIndexOutOfBoundsException {
		return metadataProperties.put(tdef.getColumns()[colpos].getName(), obj);
	}

	@Override
	public Object replace(String colname, Object obj) throws IllegalArgumentException {
		return metadataProperties.replace(colname, obj);
	}

	@Override
	public Object remove(String colname) {
		return metadataProperties.remove(colname);
	}

	@Override
	public int size() {
		return metadataProperties.size();
	}
}
