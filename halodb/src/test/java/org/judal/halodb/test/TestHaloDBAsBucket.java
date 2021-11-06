package org.judal.halodb.test;

import java.io.IOException;

import java.util.Iterator;
import java.util.Map;

import javax.jdo.JDOException;
import javax.transaction.SystemException;

import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import org.judal.storage.EngineFactory;
import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.Stored;

import org.judal.halodb.HaloDBDataSource;
import org.judal.halodb.HaloDBEngine;

public class TestHaloDBAsBucket {

	private static Map<String,String> properties;
	private static HaloDBDataSource dts;

	private final String bucketName = "unittest_bucket";

	@BeforeClass
	public static void init() throws ClassNotFoundException, JDOException, IOException {
		properties = TestHaloDB.getTestProperties();
		final HaloDBEngine hle = new HaloDBEngine();
		EngineFactory.registerEngine(EngineFactory.NAME_HALODB, hle.getClass().getName());
		dts = hle.getDataSource(properties);
		EngineFactory.DefaultThreadDataSource.set(dts);
	}

	@AfterClass
	public static void cleanup() throws JDOException {
		if (dts!=null) {
			EngineFactory.DefaultThreadDataSource.remove();
			dts.close();
		}
	}

	@Test
	public void test01Bucket() throws JDOException {
		try (Bucket bckt = getBucketDataSource().openBucket(bucketName)) {
			Stored written1 = getByteStored1();
			bckt.store(written1);
			assertTrue(bckt.exists("1"));
			assertFalse(bckt.exists("2"));
			Stored retrieved1 = newStored();
			bckt.load("1", retrieved1);
			assertEquals(written1.getKey(), retrieved1.getKey());
			assertArrayEquals((byte[]) written1.getValue(), (byte[]) retrieved1.getValue());
			Stored written2 = getByteStored2();
			bckt.store(written2);
			assertTrue(bckt.exists("1"));
			assertTrue(bckt.exists("2"));
			Stored retrieved2 = newStored();
			bckt.load("2", retrieved2);
			assertEquals(written2.getKey(), retrieved2.getKey());
			assertArrayEquals((byte[]) written2.getValue(), (byte[]) retrieved2.getValue());
			int recordCount = 0;
			Iterator<Stored> iter = bckt.iterator();
			boolean hasNxt = iter.hasNext();
			while (hasNxt) {
				++recordCount;
				iter.next();
				hasNxt = iter.hasNext();
			}
			assertEquals(2, recordCount);
			bckt.delete("1");
			assertFalse(bckt.exists("1"));
			Stored blank = newStored();
			assertFalse(bckt.load("1", blank));
			bckt.delete("2");
			assertFalse(bckt.exists("2"));
			recordCount = 0;
			iter = bckt.iterator();
			while (iter.hasNext()) {
				++recordCount;
				iter.next();
			}
			assertEquals(0, recordCount);
		}
	}

	public HaloDBDataSource getBucketDataSource() throws JDOException {
		return dts;
	}

	public Stored getByteStored1() throws JDOException {
		TestStoredHaloDB stored = newStored();
		stored.setKey("1");
		stored.setValue(new byte[]{1,2,3,4,5,6,7,8,9});
		return stored;
	}

	public Stored getByteStored2() throws JDOException {
		TestStoredHaloDB stored = newStored();
		stored.setKey("2");
		stored.setValue(new byte[]{10,11,12,13,14,15,16,17,18});
		return stored;
	}

	public TestStoredHaloDB newStored() throws JDOException {
		return new TestStoredHaloDB(bucketName);
	}
	
}
