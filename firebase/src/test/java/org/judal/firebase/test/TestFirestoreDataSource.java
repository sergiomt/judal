package org.judal.firebase.test;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.judal.firebase.FirestoreDataSource;

import static org.judal.storage.DataSource.BUCKET;
import static org.judal.storage.DataSource.PROJECTID;
import static org.judal.storage.DataSource.SECRETKEY;
import static org.judal.storage.DataSource.URI;

public class TestFirestoreDataSource {

	private static Map<String, String> properties;

	private final String collection1Name = "MyCollection1";

	@Before
	public void setUp() {
		properties = new HashMap<>();
		properties.put(BUCKET, "judal-test.appspot.com");
		properties.put(PROJECTID, "judal-test");
		properties.put(SECRETKEY, "test/judal-test-firebase.cnf");
		properties.put(URI, "https://judal-test.firebaseio.com");
	}

	@Test
	public void testDataSource() throws NumberFormatException, IOException {
		FirestoreDataSource dts = new FirestoreDataSource(properties);
		dts.close();
	}

	@Test
	public void testExists() throws NumberFormatException, IOException {
		FirestoreDataSource dts = new FirestoreDataSource(properties);
		assertTrue(dts.exists(collection1Name, "U"));
		assertFalse(dts.exists("noname", "U"));
		dts.close();
	}
}
