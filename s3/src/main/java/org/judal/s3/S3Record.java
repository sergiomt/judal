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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.sql.Types;

import com.amazonaws.services.s3.model.ObjectMetadata;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;

import javax.jdo.JDOException;
import org.judal.serialization.BytesConverter;
import org.judal.storage.java.MapRecord;

/**
 * 
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class S3Record extends MapRecord {

	private static final long serialVersionUID = 1L;
	
	private static final String content = "content";
	private static final String contentLength = "contentLength";

	public final String[] standardProperties = new String[]{"cachecontrol",content,"contentdisposition","contentencoding","contentlength","contentmd5","contenttype","etag","expirationtime","expirationtimeRuleId","httpexpiresdate","lastmodified","restoreexpirationtime","serversideencryption","versionid"};

	private String key;

	public S3Record(TableDef tableDef) throws JDOException {
		super(tableDef);
	}

	/**
	 * Constructor
	 * @param bucketName String
	 * @param columnNames String[] Metadata column names
	 * @throws JDOException
	 */
	public S3Record(String bucketName, String... columnNames) throws JDOException {
		super(new S3TableDef(bucketName));
		for (String columnName : columnNames) {
			getTableDef().addColumnMetadata("", columnName, Types.VARCHAR, true);
		}
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
		Object retval = get(columnName);
		if (columnName.equalsIgnoreCase(content)) {
			if (bytes!=null) {
				super.put(content, bytes);
				super.put(contentLength, new Long(bytes.length));
			} else {
				super.put(content, new byte[0]);
				super.put(contentLength, new Long(0l));
			}
		} else {
			if (bytes!=null)
				super.put(columnName, bytes);
			else
				super.remove(columnName);
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
		Object retval = get(columnName);
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
				super.put(content, bytes);
				super.put(contentLength, new Long(bytes.length));
			} else {
				super.put(columnName, value);
			}
		} else {
			super.remove(columnName);
			if (columnName.equalsIgnoreCase(content))
				super.put(contentLength, new Long(0l));  			
		}
		return retval;
	}

	public ObjectMetadata getMetadata() {
		ObjectMetadata oMDat = new ObjectMetadata();
		try {
			if (!isNull(content)) {
				MessageDigest oMsgd = MessageDigest.getInstance("MD5");
				Object oCnt = get(content);
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
				oMDat.addUserMetadata(sColName, get(sColName).toString());
			}
		}
		return oMDat;
	}

}
