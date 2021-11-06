package org.judal.firebase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;
import org.judal.metadata.ViewDef;

import org.judal.storage.ConstraintsChecker;
import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.FieldHelper;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.SchemalessTable;
import org.judal.storage.table.impl.AbstractRecordBase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.knowgate.gis.LatLong;

public class FirestoreDocument extends AbstractRecordBase implements Stored {

	private static final long serialVersionUID = 1L;

	private final ViewDef tdef;

	private DocumentSnapshot wrapped;

	private Map<String, Object> read;

	private Map<String, Object> writen;

	private Set<String> removed;

	private FetchGroup ftchGrp;

	private Object documentId;

	public FirestoreDocument(String collectionName) {
		this.tdef = new TableDef(collectionName);
		this.read = null;
		this.writen = null;
		this.wrapped = null;
		this.removed = null;
	}

	public FirestoreDocument(ViewDef tdef) {
		this.tdef = tdef;
		this.read = null;
		this.writen = null;
		this.wrapped = null;
		this.removed = null;
	}

	public FirestoreDocument(String collectionName, DocumentReference docref) throws JDOException {
		this.tdef = new TableDef(collectionName);
		this.writen = null;
		this.removed = null;
		try {
			this.wrapped = docref.get().get();
			this.read = wrapped.getData();
		} catch (InterruptedException | ExecutionException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	public FirestoreDocument(ViewDef tdef, DocumentReference docref) throws JDOException {
		this.tdef = tdef;
		this.writen = null;
		this.removed = null;
		try {
			wrapped = docref.get().get();
			this.read = wrapped.getData();
		} catch (InterruptedException | ExecutionException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	public FirestoreDocument(String collectionName, DocumentSnapshot docSnapshot)  {
		this.tdef = new TableDef(collectionName);
		this.wrapped = docSnapshot;
		this.read = wrapped.getData();
		this.writen = null;
		this.removed = null;
	}

	public FirestoreDocument(ViewDef tdef, DocumentSnapshot docSnapshot) {
		this.tdef = tdef;
		this.wrapped = docSnapshot;
		this.read = wrapped.getData();
		this.writen = null;
		this.removed = null;
	}

	@Override
	public ColumnDef[] columns() {
		return tdef.getColumns();
	}

	@Override
	public ColumnDef getColumn(String colname) throws ArrayIndexOutOfBoundsException {
		return tdef.getColumnByName(colname);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Entry<String, Object>[] asEntries() {
		Entry<String, Object>[] retval;
		if (null==writen && null==read) {
			retval = new Entry[0];
		} else if (read==null) {
			if (removed==null || removed.isEmpty()) {
				retval = writen.entrySet().toArray(new Entry[writen.size()]);
			} else {
				final List<Entry<String, Object>> diff = writen.entrySet().stream().filter(e -> !removed.contains(e.getKey())).collect(Collectors.toList());
				retval = diff.toArray(new Entry[diff.size()]);
			}
		} else if (writen==null) {
			if (removed==null || removed.isEmpty()) {
				retval = read.entrySet().toArray(new Entry[writen.size()]);
			} else {
				final List<Entry<String, Object>> diff = read.entrySet().stream().filter(e -> !removed.contains(e.getKey())).collect(Collectors.toList());
				retval = diff.toArray(new Entry[diff.size()]);
			}
		} else {
			final List<Entry<String, Object>> diff = new ArrayList<>(read.size()+writen.size());
			if (removed==null || removed.isEmpty()) {
				diff.addAll(writen.entrySet());
				for (Entry<String, Object> e : read.entrySet()) {
					if (!writen.containsKey(e.getKey()))
						diff.add(e);
				}
				retval = diff.toArray(new Entry[diff.size()]);
			} else {
				for (Entry<String, Object> e : writen.entrySet()) {
					if (!removed.contains(e.getKey()))
						diff.add(e);
				}
				for (Entry<String, Object> e : read.entrySet()) {
					if (!writen.containsKey(e.getKey()) && !removed.contains(e.getKey()))
						diff.add(e);
				}
				retval = diff.toArray(new Entry[diff.size()]);
			}
		}
		return retval;
	}

	@Override
	public Map<String, Object> asMap() {
		Map<String, Object> retval;
		if (null==writen && null==read) {
			retval = Collections.emptyMap();
		} else {
			if (null==writen) {
				if (null==removed || removed.isEmpty()) {
					retval = Collections.unmodifiableMap(read);
				} else {
					final Map<String, Object> diff = new HashMap<>(read.size()*2+1);
					diff.putAll(read);
					removed.forEach(key -> diff.remove(key));
					retval = Collections.unmodifiableMap(diff);
				}
			} else if (null==read) {
				if (null==removed || removed.isEmpty()) {
					retval = Collections.unmodifiableMap(writen);
				} else {
					final Map<String, Object> diff = new HashMap<>(writen.size()*2+1);
					diff.putAll(writen);
					removed.forEach(key -> diff.remove(key));
					retval = Collections.unmodifiableMap(diff);
				}
			} else {
				final Map<String, Object> diff = new HashMap<>((read.size() + writen.size())*2+1);
				diff.putAll(read);
				diff.putAll(writen);
				if (null!=removed && !removed.isEmpty())
					removed.forEach(key -> diff.remove(key));
				retval = Collections.unmodifiableMap(diff);
			}
			
		}
		return retval;
	}

	@Override
	public boolean isNull(String colname) {
		boolean retval;
		if (null==read && null==writen) {
			retval = true;
		} else if (null==writen) {
			if (null!=removed && removed.contains(colname))
				retval = true;
			else
				retval = read.get(colname)==null;
		} else if (null==read) {
			if (null!=removed && removed.contains(colname))
				retval = true;
			else
				retval = writen.get(colname)==null;
		} else {
			if (null!=removed && removed.contains(colname))
				retval = true;
			else
				retval = writen.containsKey(colname) ? writen.get(colname)==null : read.get(colname)==null;
		}
		return retval;
	}

	@Override
	public boolean isEmpty(String colname) {
		Object value = apply(colname);
		return value==null || value instanceof String && ((String) value).length()==0;
	}

	@Override
	public void clear() {
		if (null!=read) {
			if (null==removed)
				removed = new HashSet<>();
			read.keySet().forEach(key -> removed.add(key));
		}
		if (writen!=null)
			writen.clear();
	}

	@Override
	public String getTableName() {
		return tdef.getName();
	}

	@Override
	public FetchGroup fetchGroup() {
		if (ftchGrp==null)
			ftchGrp = new ColumnGroup(columns());
		return ftchGrp;
	}

	@Override
	public ConstraintsChecker getConstraintsChecker() {
		return null;
	}

	@Override
	public FieldHelper getFieldHelper() {
		return null;
	}

	@Override
	public Object apply(String colname) {
		Object retval;
		if (writen!=null) {
			if (removed!=null && removed.contains(colname)) {
				retval = null;
			} else {
				retval = writen.get(colname);
				if (null==retval && read!=null)
					retval = read.get(colname);
			}
		} else if (read!=null) {
			retval = removed!=null && removed.contains(colname) ? null : read.get(colname);
		} else {
			retval = null;
		}
		return retval;
	}

	@Override
	public int getIntervalPart(String colname, String part) throws ClassCastException, ClassNotFoundException,
			NullPointerException, NumberFormatException, IllegalArgumentException {
		throw new UnsupportedOperationException("FirestoreDocument does not support getIntervalPart()");
	}

	@Override
	public Integer[] getIntegerArray(String colname) throws ClassCastException, ClassNotFoundException {
		return (Integer[]) apply(colname);
	}

	@Override
	public Long[] getLongArray(String colname) throws ClassCastException, ClassNotFoundException {
		return (Long[]) apply(colname);
	}

	@Override
	public Float[] getFloatArray(String colname) throws ClassCastException, ClassNotFoundException {
		return (Float[]) apply(colname);
	}

	@Override
	public Double[] getDoubleArray(String colname) throws ClassCastException, ClassNotFoundException {
		return (Double[]) apply(colname);
	}

	@Override
	public Date[] getDateArray(String colname) throws ClassCastException, ClassNotFoundException {
		return (Date[]) apply(colname);
	}

	@Override
	public LatLong getLatLong(String colname) {
		return (LatLong) apply(colname);
	}

	@Override
	public String[] getStringArray(String colname) throws ClassCastException, ClassNotFoundException {
		return (String[]) apply(colname);
	}

	@Override
	public Object getMap(String colname)
			throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		return apply(colname);
	}

	@Override
	public Object put(int colpos, Object obj) throws IllegalArgumentException, ArrayIndexOutOfBoundsException, UnsupportedOperationException {
		if (columns().length==0)
			throw new UnsupportedOperationException("FirestoreDocument does not support putting values by column position");
		return put(columns()[colpos-1].getName(), obj);
	}

	@Override
	public Object put(String colname, Object obj) throws IllegalArgumentException {
		Object retval = apply(colname);
		if (null==writen)
			writen = new HashMap<>();
		writen.put(colname, obj);
		if (removed!=null)
			removed.remove(colname);
		return retval;
	}

	@Override
	public Object replace(String colname, Object obj) throws IllegalArgumentException {
		Object retval = apply(colname);
		if (null==writen) {
			writen = new HashMap<>();
			writen.put(colname, obj);
		} else {
			writen.replace(colname, obj);
		}
		if (removed!=null)
			removed.remove(colname);
		return retval;
	}

	@Override
	public Object put(String colname, byte[] bytearray) throws IllegalArgumentException {
		Object retval = apply(colname);
		if (null==writen)
			writen = new HashMap<>();
		writen.put(colname, bytearray);
		if (removed!=null)
			removed.remove(colname);
		return retval;
	}

	@Override
	public Object remove(String colname) {
		Object retval = apply(colname);
		if (writen!=null)
			writen.remove(colname);
		if (removed==null)
			removed = new HashSet<>();
		removed.add(colname);
		return retval;
	}

	@Override
	public int size() {
		int distinct;
		if (null==read && null==writen) {
			return 0;
		} else {
			if (null==read) {
				distinct = writen.size();
				if (removed!=null) {
					Iterator<String> removedKeys = removed.iterator();
					while (removedKeys.hasNext())
						if (writen.containsKey(removedKeys.next()))
							distinct--;
				}
			} else if (null==writen) {
				distinct = read.size();
				if (removed!=null) {
					Iterator<String> removedKeys = removed.iterator();
					while (removedKeys.hasNext())
						if (read.containsKey(removedKeys.next()))
							distinct--;
				}
			} else {
				distinct = read.size();
				for (String key : writen.keySet())
					if (!read.containsKey(key))
						distinct++;
				if (removed!=null) {
					Iterator<String> removedKeys = removed.iterator();
					while (removedKeys.hasNext())
						if (read.containsKey(removedKeys.next()) || writen.containsKey(removedKeys.next()))
							distinct--;
				}
			}
		}
		return distinct;
	}

	@Override
	public void setKey(Object key) throws JDOException {
		documentId = key;
	}

	@Override
	public Object getKey() throws JDOException {
		return documentId;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setValue(Serializable value) throws JDOException, ClassCastException {
		if (null==value) {
			wrapped = null;
			read = null;
			writen = null;
			removed = null;
		}
		else if (value instanceof DocumentSnapshot) {
			wrapped = (DocumentSnapshot) value;
			read = wrapped.getData();
			if (null!=writen)
				writen.clear();
			removed = null;
		} else if (value instanceof byte[]) {
			try (ByteArrayInputStream bin = new ByteArrayInputStream((byte[]) value)) {
				try (ObjectInputStream oin = new ObjectInputStream(bin)) {
					Object readed = oin.readObject();
					if (readed instanceof DocumentSnapshot) {
						wrapped = (DocumentSnapshot) oin.readObject();
						read = wrapped.getData();
						if (null!=writen)
							writen.clear();
						removed = null;
					} else if (readed instanceof Map) {
						writen = (Map<String, Object>) readed;
						wrapped = null;
						read = null;
						removed = null;
					} else {
						throw new ClassCastException("Unparseable class " + readed!=null ? readed.getClass().getName() : " null");
					}
				}
			} catch (ClassNotFoundException | IOException e) {
				throw new JDOException(e.getMessage(), e);
			}
		} else if (value instanceof Map) {
			writen = (Map<String,Object>) value;
		} else if (value instanceof String) {
			wrapped = null;
			ObjectMapper mapper = new ObjectMapper();
			try {
				writen = mapper.readValue((String) value, Map.class);
			} catch (IOException e) {
				throw new JDOException(e.getMessage(), e);
			}

		} else {
			throw new ClassCastException("Value must be of type DocumentSnapshot");
		}
	}

	@Override
	public void setContent(byte[] bytes, String contentType) throws JDOException {
		if ("application/json".equalsIgnoreCase(contentType)) {
			try {
				setValue(new String(bytes, "UTF-8"));
			} catch (UnsupportedEncodingException neverthrown) { }
		} else if ("application/octet-stream".equalsIgnoreCase(contentType)) {
			setValue(bytes);
		} else {
			throw new IllegalArgumentException("Unsupported content type " + contentType);
		}
	}

	@Override
	public Object getValue() throws JDOException {
		byte[] value;
		try (ByteArrayOutputStream bout = new ByteArrayOutputStream()) {
			try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
				oout.writeObject(asMap());
			}
			value = bout.toByteArray();
		} catch (IOException e) {
			throw new JDOException(e.getMessage(), e);
		}
		return value;
	}

	@Override
	public String getBucketName() {
		return tdef.getName();
	}

	@Override
	public boolean load(Object key) throws JDOException {
		return load(EngineFactory.getDefaultTableDataSource(), key);
	}

	@Override
	public boolean load(DataSource dataSource, Object key) throws JDOException {
		FirestoreSchemalessDataSource fdts = (FirestoreSchemalessDataSource) dataSource;
		boolean retval;
		try (SchemalessTable table = fdts.openTable(this)) {
			retval = table.load(key, this);
		}
		return retval;
	}

	@Override
	public void store() throws JDOException {
		store(EngineFactory.getDefaultTableDataSource());
	}

	@Override
	public void store(DataSource dataSource) throws JDOException {
		FirestoreSchemalessDataSource fdts = (FirestoreSchemalessDataSource) dataSource;
		try (SchemalessTable table = fdts.openTable(this)) {
			table.store(this);
		}
	}

	@Override
	public void delete(DataSource dataSource) throws JDOException {
		FirestoreSchemalessDataSource fdts = (FirestoreSchemalessDataSource) dataSource;
		try (SchemalessTable table = fdts.openTable(this)) {
			table.delete(getKey());
		}
	}

	@Override
	public void delete() throws JDOException {
		delete(EngineFactory.getDefaultTableDataSource());
	}

}
