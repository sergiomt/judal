package org.judal.firebase.test;

import org.junit.Before;
import org.junit.Test;
import org.threeten.bp.LocalDate;
import org.junit.Ignore;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.judal.firebase.FirestoreDocument;
import org.judal.storage.table.SchemalessTable;
import org.judal.firebase.FirestoreSchemalessDataSource;

import static org.judal.storage.DataSource.BUCKET;
import static org.judal.storage.DataSource.PROJECTID;
import static org.judal.storage.DataSource.SECRETKEY;
import static org.judal.storage.DataSource.URI;

public class TestFirestoreSchemalessTable {

	private static Map<String, String> properties;

	private Random rnd = new Random();
	int randomInt1, randomInt2;

	@Before
	public void setUp() {
		properties = new HashMap<>();
		properties.put(BUCKET, "judal-test.appspot.com");
		properties.put(PROJECTID, "judal-test");
		properties.put(SECRETKEY, "test/judal-test-firebase.cnf");
		properties.put(URI, "https://judal-test.firebaseio.com");
		randomInt1 = rnd.nextInt();
		randomInt2 = rnd.nextInt();
	}

	@Test
	public void testVoid() throws IOException {
		LocalDate d = LocalDate.now();
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		ObjectOutputStream oout = new  ObjectOutputStream(bout);
		oout.writeObject(d);
		byte[] bts = bout.toByteArray();
		System.out.println("Serialized LocalDate is " + bts.length + " bytes");
		oout.close();
		bout.close();
		LocalDateTime dt = LocalDateTime.now();
		bout = new ByteArrayOutputStream();
		oout = new  ObjectOutputStream(bout);
		oout.writeObject(d);
		bts = bout.toByteArray();
		System.out.println("Serialized LocalDateTime is " + bts.length + " bytes");
		oout.close();
		bout.close();
	}
	
	@Ignore
	public void testWriteRead() throws NumberFormatException, IOException {
		try (FirestoreSchemalessDataSource dts = new FirestoreSchemalessDataSource(properties)) {
			FirestoreDocument rec = new FirestoreDocument("MyCollection1");
			SchemalessTable c = dts.openTable(rec);

			rec.setKey("id4");
			rec.setValue("{\"nameof\":\"Test Name for ID 4\", \"random\":\"" + randomInt1 + "\"}");
			c.store(rec);
			rec = new FirestoreDocument("MyCollection1");
			assertTrue(c.load("id4", rec));
			assertEquals("Test Name for ID 4", rec.getString("nameof"));
			assertEquals(randomInt1, rec.getInt("random"));

			rec = new FirestoreDocument("MyCollection1");
			randomInt2 = rnd.nextInt();
			rec.setKey("id5");
			rec.put("nameof", "Test Name for ID 5");
			rec.put("random", randomInt2);
			c.store(rec);
			rec = new FirestoreDocument("MyCollection1");
			assertTrue(c.load("id5", rec));
			assertEquals("Test Name for ID 5", rec.getString("nameof"));
			assertEquals(randomInt2, rec.getInt("random"));

			c.close();
			dts.close();
		}
	}

}
