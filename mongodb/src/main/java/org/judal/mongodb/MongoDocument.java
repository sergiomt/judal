package org.judal.mongodb;

/**
 * © Copyright 2018 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;

import java.util.Base64;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.bson.Document;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;
import org.judal.metadata.ViewDef;
import org.judal.storage.ConstraintsChecker;
import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.FieldHelper;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.impl.AbstractRecordBase;

import com.knowgate.gis.LatLong;

public class MongoDocument extends AbstractRecordBase implements Stored {

	private static final long serialVersionUID = 1L;

	private ViewDef tdef;

	private Document wrapped;
	
	private FetchGroup ftchGrp;

	public MongoDocument(String collectionName) {
		this.tdef = new TableDef(collectionName);
	}

	public MongoDocument(ViewDef tdef) {
		this.tdef = tdef;
	}

	public MongoDocument(String collectionName, Document bsonDocument) {
		this.tdef = new TableDef(collectionName);
				this.wrapped = bsonDocument;
	}

	public MongoDocument(ViewDef tdef, Document bsonDocument) {
		this.tdef = tdef;
		this.wrapped = bsonDocument;
	}

	@Override
	public void setKey(Object key) throws JDOException {
		wrapped.put("_id", key);
	}

	@Override
	public Object getKey() throws JDOException {
		return wrapped.get("_id");
	}

	@Override
	public void setValue(Serializable value) throws JDOException, ClassCastException {
		if (null==value)
			wrapped = new Document();
		else if (value instanceof Document) {
			wrapped = (Document) value;
		} else if (value instanceof byte[]) {
			try (ByteArrayInputStream bin = new ByteArrayInputStream((byte[]) value)) {
				try (ObjectInputStream oin = new ObjectInputStream(bin)) {
					Object readed = oin.readObject();
					if (readed instanceof Document) {
						wrapped = (Document) oin.readObject();
					} else if (readed instanceof Map) {
						wrapped = new Document();
						wrapped.putAll((Map) value);
					} else {
						throw new ClassCastException("Unparseable class " + readed!=null ? readed.getClass().getName() : " null");
					}
				}
			} catch (ClassNotFoundException | IOException e) {
				throw new JDOException(e.getMessage(), e);
			}
		} else if (value instanceof Map) {
			wrapped = new Document();
			wrapped.putAll((Map) value);
		} else if (value instanceof String) {
			wrapped = Document.parse((String) value);
		} else {
			throw new ClassCastException("Value must be of type org.bson.Document");
		}
	}

	@Override
	public void setContent(byte[] bytes, String contentType) throws JDOException, IllegalArgumentException {
		if ("application/json".equalsIgnoreCase(contentType)) {
			try {
				wrapped = Document.parse(new String(bytes, "UTF-8"));
			} catch (UnsupportedEncodingException neverthrown) { }
		} else if ("application/octet-stream".equalsIgnoreCase(contentType)) {
			setValue(bytes);
		} else {
			throw new IllegalArgumentException("Unsupported content type " + contentType);
		}
	}

	@Override
	public byte[] getValue() throws JDOException {
		byte[] value;
		try (ByteArrayOutputStream bout = new ByteArrayOutputStream()) {
			try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
				oout.writeObject(wrapped);
			}
			value = bout.toByteArray();
		} catch (IOException e) {
			throw new JDOException(e.getMessage(), e);
		}
		return value;
	}

	public Document getDocument() {
		return wrapped;
	}

	public void setDocument(Document doc) {
		wrapped = doc;
	}

	@Override
	public String toJSON() {
		return wrapped.toJson();
	}

	@Override
	public String getBucketName() {
		return tdef.getName();
	}

	@Override
	public boolean load(Object key) throws JDOException {
		return load(EngineFactory.getDefaultBucketDataSource(), key);
	}

	@Override
	public boolean load(DataSource dataSource, Object key) throws JDOException {
		MongoDataSource fdts = (MongoDataSource) dataSource;
		boolean retval;
		try (MongoBucket bucket = fdts.openBucket(tdef.getName())) {
			retval = bucket.load(key, this);
		}
		return retval;
	}

	@Override
	public void store() throws JDOException {
		store(EngineFactory.getDefaultBucketDataSource());
	}

	@Override
	public void store(DataSource dataSource) throws JDOException {
		MongoDataSource fdts = (MongoDataSource) dataSource;
		try (MongoBucket bucket = fdts.openBucket(tdef.getName())) {
			bucket.store(this);
		}
	}

	@Override
	public void delete(DataSource dataSource) throws JDOException {
		MongoDataSource fdts = (MongoDataSource) dataSource;
		try (MongoBucket bucket = fdts.openBucket(tdef.getName())) {
			bucket.delete(getKey());
		}
	}

	@Override
	public void delete() throws JDOException {
		delete(EngineFactory.getDefaultBucketDataSource());
	}

	@Override
	public Object apply(String key) {
		return wrapped.get(key);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Entry<String, Object>[] asEntries() {
		return wrapped.entrySet().toArray(new Entry[wrapped.entrySet().size()]);
	}

	@Override
	public Map<String, Object> asMap() {
		return wrapped;
	}

	@Override
	public void clear() {
		wrapped.clear();
	}

	@Override
	public ColumnDef[] columns() {
		return tdef.getColumns();
	}

	@Override
	public FetchGroup fetchGroup() {
		if (ftchGrp==null)
			ftchGrp = new ColumnGroup(columns());
		return ftchGrp;
	}

	@Override
	public boolean getBoolean(String key, boolean defaultValue) throws ClassCastException {
		return wrapped.containsKey(key) ? wrapped.getBoolean(key) : defaultValue;
	}

	@Override
	public byte[] getBytes(String key) throws ClassCastException, IllegalArgumentException {
		byte[] retval = null;
		if (wrapped.containsKey(key))
			retval = Base64.getDecoder().decode(wrapped.getString(key));
		return retval;
	}

	@Override
	public Calendar getCalendar(String key) throws ClassCastException {
		Calendar retval = null;
		if (wrapped.containsKey(key)) {
			retval = new GregorianCalendar();
			retval.setTime(wrapped.getDate(key));
		}
		return retval;
	}

	@Override
	public ColumnDef getColumn(String columnName) throws ArrayIndexOutOfBoundsException {
		return tdef.getColumnByName(columnName);
	}

	@Override
	public ConstraintsChecker getConstraintsChecker() {
		return null;
	}

	@Override
	public Date getDate(String key) {
		return wrapped.containsKey(key) ? wrapped.getDate(key) : null;
	}

	@Override
	public Date getDate(String key, Date defaultValue) throws ClassCastException {
		return wrapped.containsKey(key) ? wrapped.getDate(key) : defaultValue;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Date[] getDateArray(String key) throws ClassCastException, ClassNotFoundException {
		Date[] retval = null;
		if (wrapped.containsKey(key)) {
			List<Date> dates = (List<Date>) wrapped.get(key);
			retval = dates.toArray(new Date[dates.size()]);
		}
		return retval;
	}

	@Override
	public boolean isNull(String key) {
		return !wrapped.containsKey(key);
	}

	@Override
	public boolean isEmpty(String key) {
		return isNull(key) || wrapped.getString(key).length()==0;
	}

	@Override
	public String getTableName() {
		return tdef.getName();
	}

	@Override
	public FieldHelper getFieldHelper() {
		return null;
	}

	@Override
	public int getIntervalPart(String key, String part) throws ClassCastException, ClassNotFoundException,
			NullPointerException, NumberFormatException, IllegalArgumentException,UnsupportedOperationException {
		throw new UnsupportedOperationException("MongoDocument does not support getIntervalPart()");
	}

	@Override
	@SuppressWarnings("unchecked")
	public Integer[] getIntegerArray(String key) throws ClassCastException, ClassNotFoundException {
		Integer[] retval = null;
		if (wrapped.containsKey(key)) {
			List<Integer> ints = (List<Integer>) wrapped.get(key);
			retval = ints.toArray(new Integer[ints.size()]);
		}
		return retval;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Long[] getLongArray(String key) throws ClassCastException, ClassNotFoundException {
		Long[] retval = null;
		if (wrapped.containsKey(key)) {
			List<Long> lngs = (List<Long>) wrapped.get(key);
			retval = lngs.toArray(new Long[lngs.size()]);
		}
		return retval;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Float[] getFloatArray(String key) throws ClassCastException, ClassNotFoundException {
		Float[] retval = null;
		if (wrapped.containsKey(key)) {
			List<Float> flts = (List<Float>) wrapped.get(key);
			retval = flts.toArray(new Float[flts.size()]);
		}
		return retval;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Double[] getDoubleArray(String key) throws ClassCastException, ClassNotFoundException {
		Double[] retval = null;
		if (wrapped.containsKey(key)) {
			List<Double> dbls = (List<Double>) wrapped.get(key);
			retval = dbls.toArray(new Double[dbls.size()]);
		}
		return retval;
	}

	@SuppressWarnings("unchecked")
	@Override
	public String[] getStringArray(String key) throws ClassCastException, ClassNotFoundException {
		String[] retval = null;
		if (wrapped.containsKey(key)) {
			List<String> dbls = (List<String>) wrapped.get(key);
			retval = dbls.toArray(new String[dbls.size()]);
		}
		return retval;
	}

	@Override
	public LatLong getLatLong(String key)
			throws ClassCastException, NumberFormatException, ArrayIndexOutOfBoundsException, ClassNotFoundException {
		LatLong retval = null;
		if (wrapped.containsKey(key)) {
			Float[] coords = getFloatArray(key);
			retval = new LatLong(coords[0], coords[1]);
		}
		return retval;
	}

	@Override
	public Object getMap(String key) {
		return wrapped.containsKey(key) ? wrapped.get(key) : null;
	}

	@Override
	public Object put(int colpos, Object obj) throws IllegalArgumentException, ArrayIndexOutOfBoundsException, UnsupportedOperationException {
		if (columns().length==0)
			throw new UnsupportedOperationException("MongoDocument does not support putting values by column position");
		return put(columns()[colpos-1].getName(), obj);
	}

	@Override
	public Object put(String key, Object value) throws IllegalArgumentException {
		Object formerValue = wrapped.get(key);
		wrapped.put(key, value);
		return formerValue;
	}

	@Override
	public Object replace(String key, Object value) throws IllegalArgumentException {
		Object formerValue = wrapped.get(key);
		wrapped.replace(key, value);
		return formerValue;
	}

	public Object replace(String key, byte[] bytearray) throws IllegalArgumentException {
		Object formerValue = wrapped.get(key);
		wrapped.replace(key, Base64.getEncoder().encodeToString(bytearray));
		return formerValue;
	}

	@Override
	public Object put(String key, byte[] bytearray) throws IllegalArgumentException {
		Object formerValue = wrapped.get(key);
		wrapped.put(key, Base64.getEncoder().encodeToString(bytearray));
		return formerValue;
	}

	@Override
	public Object remove(String key) {
		return wrapped.remove(key);
	}

	@Override
	public int size() {
		return wrapped.size();
	}

}
