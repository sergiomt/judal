package org.judal.mongodb.test;

import org.junit.Test;
import org.junit.Ignore;
import org.junit.Before;
import org.junit.After;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.judal.mongodb.MongoDataSource;
import org.judal.mongodb.MongoSequence;

public class TestMongoSequence extends TestMongoBase {

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
	public void testSequence() throws InterruptedException {
		Map<String, String> properties = new HashMap<>();
		try (MongoDataSource dts = new MongoDataSource(properties)) {
			MongoSequence seq = dts.createSequence("seqTest", 10);
			assertEquals(10l, seq.currentValue());
			assertEquals(11l, seq.nextValue());
			Thread t1 = new Thread() {
				@Override
				public void run() {
					MongoSequence seq1 = dts.getSequence("seqTest");
					for (long l=0; l<1000; l++)
						seq1.next();
				}
			};
			Thread t2 = new Thread() {
				@Override
				public void run() {
					MongoSequence seq2 = dts.getSequence("seqTest");
					for (long l=0; l<1000; l++)
						seq2.next();
				}
			};
			Thread t3 = new Thread() {
				@Override
				public void run() {
					MongoSequence seq3 = dts.getSequence("seqTest");
					for (long l=0; l<1000; l++)
						seq3.next();
				}
			};
			t1.start();
			t2.start();
			t3.start();
			t1.join();
			t2.join();
			t3.join();
			assertEquals(3011l, seq.currentValue());
			dts.dropSequence("seqTest");
		}
	}

}
