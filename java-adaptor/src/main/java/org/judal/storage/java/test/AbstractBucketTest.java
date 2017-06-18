package org.judal.storage.java.test;

import java.io.IOException;
import java.sql.Types;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import org.judal.serialization.BytesConverter;

import javax.jdo.JDOException;
import javax.transaction.Status;
import javax.transaction.SystemException;

import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.BucketDataSource;
import org.judal.storage.keyvalue.Stored;

public abstract class AbstractBucketTest {

	public static String bucketName = "unittest_bucket";
	public static String pk = "Stored001";
	
	public abstract BucketDataSource getBucketDataSource() throws JDOException;
	
	public abstract Stored getByteStored1() throws JDOException;

	public abstract Stored getObjectStored2() throws JDOException;

	public abstract Stored getRetrieved() throws JDOException;

	public void test01Bucket() throws JDOException, IOException, SystemException {
		BucketDataSource ds = null;
		Bucket bc = null;
		Stored retrieved;
		boolean created = false;
		try {
			ds = getBucketDataSource();

			if (ds.exists(bucketName, "U")) {
				System.out.println("Bucket "+bucketName+" already exists");
				created = false;
			} else {
				System.out.println("Creating bucket "+bucketName);
				ds.createBucket(bucketName, null);
				created = true;
			}
			assertTrue(ds.exists(bucketName, "U"));
			bc = ds.openBucket(bucketName);
			Stored toStor = getByteStored1();
			System.out.println("data to write is "+toStor);
			bc.store(toStor);
			System.out.println("stored 1");
			retrieved = getRetrieved();
			assertTrue(bc.load(pk, retrieved));
			System.out.println("reloaded 1");
			assertArrayEquals((byte[]) getByteStored1().getValue(), (byte[]) retrieved.getValue());
			bc.delete(pk);
			System.out.println("deleted 1");
			assertFalse(bc.load(pk, retrieved));
			bc.store(getObjectStored2());
			System.out.println("stored 2");
			retrieved = getRetrieved();
			assertTrue(bc.load(pk, retrieved));
			System.out.println("reloaded 2");
			HashMap<String,String> map1 = (HashMap<String,String>) BytesConverter.fromBytes((byte[]) retrieved.getValue(), Types.JAVA_OBJECT);
			HashMap<String,String> map2 = new HashMap<String,String>();
			map2.put("1", "one");
			map2.put("2", "two");
			map2.put("3", "three");
			assertEquals(map1.get("1"),map2.get("1"));
			assertEquals(map1.get("2"),map2.get("2"));
			assertEquals(map1.get("3"),map2.get("3"));
			assertFalse(bc.load("no valid pk", retrieved));
			System.out.println("reloaded 3");
			bc.close();
			bc = null;
			System.out.println("closed bucket");
			ds.truncateBucket(bucketName);
			System.out.println("truncated bucket");
			bc = ds.openBucket(bucketName);
			assertFalse(bc.load(pk, retrieved));
			bc.close();
			bc = null;
			System.out.println("not reloaded");
		} finally {
			if (bc!=null) bc.close();
			if ((ds!=null) && created) ds.dropBucket(bucketName);
		}
	}

	public String statusString(int status) {
		switch (status) {
		case Status.STATUS_NO_TRANSACTION:
			return "no transaction";
		case Status.STATUS_ACTIVE:
			return "active";
		case Status.STATUS_PREPARING:
			return "preparing";
		case Status.STATUS_PREPARED:
			return "prepared";
		case Status.STATUS_COMMITTING:
			return "committing";
		case Status.STATUS_COMMITTED:
			return "commited";
		case Status.STATUS_MARKED_ROLLBACK:
			return "marked rollback";
		case Status.STATUS_ROLLEDBACK:
			return "rolledback";
		case Status.STATUS_ROLLING_BACK:
			return "rolling back";
		default:
			return "unknown";
		}
	}
	
}