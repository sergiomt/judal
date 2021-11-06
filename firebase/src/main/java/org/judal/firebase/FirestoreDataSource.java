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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.util.Collections;
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;

import javax.transaction.TransactionManager;

import org.judal.storage.DataSource;

import org.judal.storage.Param;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;


public class FirestoreDataSource implements DataSource {

	private final FirebaseApp app;

	private final Firestore st;

	private final Map<String, String> props;

	public FirestoreDataSource (Map<String, String> properties)
			throws IOException, NumberFormatException {

			props = Collections.unmodifiableMap(properties);

			FirebaseOptions.Builder options = FirebaseOptions.builder();

			for (Map.Entry<String, String> property : properties.entrySet()) {
				switch (property.getKey()) {
				case BUCKET:
					options.setStorageBucket(property.getValue().trim());
					break;
				case CONNECTIONTIMEOUT:
					options.setConnectTimeout(Integer.parseInt(property.getValue().trim()));
					break;
				case PROJECTID:
					options.setProjectId(property.getValue().trim());
					break;
				case SECRETKEY:
					try (InputStream privateKeyJson = getClass().getResourceAsStream(property.getValue())) {
						if (null==privateKeyJson)
							throw new FileNotFoundException(property.getValue());
						options.setCredentials(GoogleCredentials.fromStream(privateKeyJson));
					}
					break;
				}
			}

			app = FirebaseApp.initializeApp(options.build());

			st = FirestoreClient.getFirestore();
	}

	@Override
	public boolean exists(String collectionName, String objectType) throws JDOException {
		return null!=st.collection(collectionName);
	}

	@Override
	public Map<String, String> getProperties() {
		return props;
	}

	@Override
	public TransactionManager getTransactionManager() {
		return null;
	}

	@Override
	public JDOConnection getJdoConnection() throws JDOException {
		return null;
	}

	@Override
	public Sequence getSequence(String name) throws JDOException {
		return null;
	}

	@Override
	public Object call(String statement, Param... parameters) throws JDOException {
		throw new JDOUnsupportedOptionException("FirestoreDataSource does not support callable statements");
	}

	@Override
	public boolean inTransaction() throws JDOException {
		return false;
	}

	@Override
	public void close() throws JDOException {
		try {
			st.close();
		} catch (Exception e) {
			throw new JDOException(e.getMessage(), e);
		}
		app.delete();
	}

	public Firestore getDb() {
		return st;
	}
}
