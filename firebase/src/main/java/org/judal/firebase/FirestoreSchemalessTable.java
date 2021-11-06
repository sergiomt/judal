package org.judal.firebase;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.metadata.PrimaryKeyMetadata;

import org.judal.metadata.IndexDef.Using;
import org.judal.storage.Param;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.Record;
import org.judal.storage.table.SchemalessIndexableTable;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.SetOptions;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;

public class FirestoreSchemalessTable extends FirestoreSchemalessView implements SchemalessIndexableTable {

	private PrimaryKeyMetadata pk;

	public FirestoreSchemalessTable(FirestoreSchemalessDataSource dataSource, String collectionName, Class<? extends Record> recordClass) {
		super(dataSource, collectionName, recordClass);
	}

	public FirestoreSchemalessTable(FirestoreSchemalessDataSource dataSource, String collectionName) {
		this(dataSource, collectionName, FirestoreDocument.class);
	}

	@Override
	public PrimaryKeyMetadata getPrimaryKey() {
		return pk;
	}

	public void setPrimaryKey(PrimaryKeyMetadata primaryKey) {
		pk = primaryKey;
	}

	@Override
	public void insert(Param... params) throws JDOException {
		Map<String, Object> data = new HashMap<>(params.length*2+1);
		String path = null;
		for (Param p : params) {
			if (p.isPrimaryKey()) {
				if (p.getValue()==null)
					throw new JDOException("FirestoreCollection.insert() Document primary key " + p.getName() + " must not be null");
				if (null==path)
					path = p.getValue().toString();
				else
					throw new JDOException("FirestoreCollection.insert() Document primary key can only be composed of one value but multiple were specified");
			}
			data.put(p.getName(), p.getValue());
		}
		if (null==path)
			throw new JDOException("FirestoreCollection.insert() Document primary key not specified");
		collection.document(path).create(data);
	}

	@Override
	public void store(Stored target) throws JDOException {
		assert(target.getKey() instanceof String);
		Map<String, Object> data;
		if (target instanceof Record) {
			data = ((Record) target).asMap();
		} else {
			data = new HashMap<>();
			data.put("key", target.getKey());
			data.put("data", target.getValue());
		}
		collection.document(target.getKey().toString()).set(data, SetOptions.merge());
	}

	@Override
	public void delete(Object key) throws JDOException {
		collection.document(key.toString()).delete();
	}

	@Override
	public int update(Param[] values, Param[] where) throws JDOException {
		int updated = 0;
		Map<String, Object> data = new HashMap<>(values.length*2);
		for (Param p : where) {
			data.put(p.getName(), p.getValue());
		}
		Query qry = getDb().collection(name());
		for (Param p : where) {
			qry.whereEqualTo(p.getName(), p.getValue());
		}
		try {
			for (QueryDocumentSnapshot d  : qry.get().get()) {
				collection.document(d.getId()).update(data);
				updated++;
			}
		} catch (InterruptedException | ExecutionException iee) {
			throw new JDOException(iee.getMessage(), iee);
		}
		return updated;
	}

	@Override
	public int delete(Param[] where) throws JDOException {
		int deleted = 0;
		Query qry = getDb().collection(name());
		for (Param p : where) {
			qry.whereEqualTo(p.getName(), p.getValue());
		}
		try {
			for (QueryDocumentSnapshot d  : qry.get().get()) {
				collection.document(d.getId()).delete();
				deleted++;
			}
		} catch (InterruptedException | ExecutionException iee) {
			throw new JDOException(iee.getMessage(), iee);
		}
		return deleted;
	}

	@Override
	public void createIndex(String indexName, boolean unique, Using indexUsing, String... columns) throws JDOException {
		throw new JDOUnsupportedOptionException("Firestore does not support creating indexes progarmmatically. Use Google console instead.");
	}

	@Override
	public void dropIndex(String indexName) throws JDOException {
		throw new JDOUnsupportedOptionException("Firestore does not support droping indexes progarmmatically. Use Google console instead.");
	}

}
