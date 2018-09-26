package org.judal.mongodb.test;

import org.junit.Test;

import org.junit.Ignore;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.judal.mongodb.MongoBucket;
import org.judal.mongodb.MongoDataSource;
import org.judal.mongodb.MongoDocument;
import org.judal.storage.keyvalue.BucketDataSource;

public class TestMongoBucket extends TestMongoBase {

	// @Before
	public void setUp() {
		super.setUp();
	}

	// @After
	public void tearDown() {
		super.tearDown();
	}

	@Test
	public void testVoid() {
	}

	@Ignore
	public void testBucket() {
		Map<String, String> properties = new HashMap<>();
		try (BucketDataSource dts = new MongoDataSource(properties)) {
			assertTrue(dts.exists(collectionName, "U"));
			try (MongoBucket bckt = (MongoBucket) dts.openBucket(collectionName)) {
				for (int d = 0; d<doc_count; d++)
					assertTrue(bckt.exists(String.valueOf(d)));
				assertEquals((long) doc_count, collection.count());
				MongoDocument target = new MongoDocument(collectionName);
				assertTrue(bckt.load("1", target));
				assertEquals("1",target.getKey());
				final AtomicInteger count = new AtomicInteger(0);
				bckt.forEach(s -> count.incrementAndGet());
				assertEquals(doc_count, count.get());
				bckt.delete("1");
				count.set(0);
				bckt.forEach(s -> count.incrementAndGet());
				assertEquals(doc_count-1, count.get());
				bckt.delete("2");
				count.set(0);
				bckt.forEach(s -> count.incrementAndGet());
				assertEquals(doc_count-2, count.get());
				bckt.delete("key not found");
				count.set(0);
				bckt.forEach(s -> count.incrementAndGet());
				assertEquals(doc_count-2, count.get());
				dts.truncateBucket(collectionName);
				count.set(0);
				bckt.forEach(s -> count.incrementAndGet());
				assertEquals(0l, count.get());
			}
		}
	}
}
