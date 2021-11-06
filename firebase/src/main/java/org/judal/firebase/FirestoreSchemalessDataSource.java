package org.judal.firebase;

/**
 * © Copyright 2019 the original author.
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
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;

import org.judal.metadata.IndexDef;
import org.judal.metadata.IndexDef.Type;
import org.judal.metadata.IndexDef.Using;
import org.judal.metadata.JoinType;
import org.judal.metadata.NameAlias;
import org.judal.storage.FieldHelper;
import org.judal.storage.table.Record;
import org.judal.storage.table.SchemalessIndexableTable;
import org.judal.storage.table.SchemalessIndexableView;
import org.judal.storage.table.SchemalessTable;
import org.judal.storage.table.SchemalessTableDataSource;
import org.judal.storage.table.SchemalessView;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.knowgate.tuples.Pair;

public class FirestoreSchemalessDataSource extends FirestoreDataSource implements SchemalessTableDataSource {

	public FirestoreSchemalessDataSource(Map<String, String> properties) throws IOException, NumberFormatException {
		super(properties);
	}

	@Override
	public IndexDef createIndexDef(String indexName, String tableName, Iterable<String> columns, Type indexType,
			Using using) throws JDOException {
		throw new JDOUnsupportedOptionException("FirestoreSchemalessDataSource does not support creation of indexes programmatically. Use Google console.");
	}

	@Override
	public void createTable(String tableName, Map<String, Object> options) throws JDOException {
		// Firestore creates collections and documents implicitly the first time you add data to the document.
		// There is no need to explicitly create collections or documents.
	}

	@Override
	public void dropTable(String tableName, boolean cascade) throws JDOException {
		truncateCollection(getDb().collection(tableName), cascade);
	}

	@Override
	public void truncateTable(String tableName, boolean cascade) throws JDOException {
		truncateCollection(getDb().collection(tableName), cascade);
	}

	private void truncateCollection(CollectionReference collref, boolean cascade) {
		for (DocumentReference docref : collref.listDocuments()) {
			if (cascade) {
				for (CollectionReference subcollref : docref.listCollections()) {
					truncateCollection(subcollref, true);
				}
			}
			docref.delete();
		}
	}

	@Override
	public SchemalessTable openTable(Record recordInstance) throws JDOException {
		return new FirestoreSchemalessTable(this, recordInstance.getTableName(), recordInstance.getClass());
	}

	@Override
	public SchemalessIndexableTable openIndexedTable(Record recordInstance) throws JDOException {
		return new FirestoreSchemalessTable(this, recordInstance.getTableName(), recordInstance.getClass());
	}

	@Override
	public SchemalessView openView(Record recordInstance) throws JDOException {
		return new FirestoreSchemalessView(this, recordInstance.getTableName(), recordInstance.getClass());
	}

	@Override
	public SchemalessIndexableView openIndexedView(Record recordInstance) throws JDOException {
		return new FirestoreSchemalessTable(this, recordInstance.getTableName(), recordInstance.getClass());
	}

	@Override
	public SchemalessIndexableView openJoinView(JoinType joinType, Record result, NameAlias baseTable,
			NameAlias joinedTable, Pair<String, String>... onColumns) throws JDOException {
		throw new JDOUnsupportedOptionException("Firestore does not support JOIN.");

	}

	@Override
	public FieldHelper getFieldHelper() throws JDOException {
		return null;
	}

}
