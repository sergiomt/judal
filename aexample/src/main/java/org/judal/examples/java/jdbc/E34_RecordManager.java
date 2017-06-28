package org.judal.examples.java.jdbc;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertEquals;


import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import org.judal.storage.EngineFactory;
import org.judal.storage.java.TableOperation;
import org.judal.storage.queue.RecordQueueProducer;
import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordManager;

import org.judal.ramqueue.RAMQueueProducer;
import org.judal.examples.java.model.pojo.Course;
import org.judal.jdbc.JDBCEngine;

/**
 * Example of how to use the RecordManager to write, read and delete records.
 * Write and delete operations are asynchronous using an in-memory queue.
 */
public class E34_RecordManager {
	@Test
	public void demo() throws Exception {
		
		setUp();

		final Cache<Object, Record> cache = createCache();

		final String engineName = new JDBCEngine().name();

		final RelationalDataSource dataSource = EngineFactory.getDefaultRelationalDataSource(); 
		final Map<String,String> properties = dataSource.getProperties();
		
		RecordQueueProducer producer = new RAMQueueProducer(engineName, properties);

		try (RecordManager manager = new RecordManager(dataSource, producer, cache, properties)) {
			
			Integer k = new Integer(1);
			Course c = new Course(dataSource);
			c.setId(k);
			c.setCode("AI07");
			c.setCourseName("Artificial Intelligence for Dummies");
			c.setPrice(new BigDecimal("1200"));
			Calendar startd = new GregorianCalendar();
			startd.setTime(new Date());
			c.setStartDate(startd);
			Calendar endd = new GregorianCalendar();
			endd.setTime(new Date(startd.getTimeInMillis()+(15l*86400000l)));
			c.setEndDate(endd);
			
			assertNotNull(c.getKey());
			
			manager.makePersistent(c);
			
			// Manager is asynchronous by default,
			// so wait for Record to be written
			// Active wait for demo purposes only
			// don't do while (!op.exists()) in production
			final long waitms = 100l;
			final long timeout = 2000l;
			long elapsed = 0l;
			try (TableOperation<Course> op = new TableOperation<Course>(dataSource, c)) {
				while (!op.exists(k) && elapsed<timeout) {
					Thread.sleep(waitms);
					elapsed += waitms;
				}
			}
			
			assertNotEquals(timeout, elapsed);
			
			Course z = new Course();
			z.setId(k);
			
			manager.retrieve(z);
			
			assertEquals(c.getCode(), z.getCode());
			assertEquals(c.getStartDate(), z.getStartDate());
			
			manager.deletePersistent(c);
			
			elapsed = 0l;
			try (TableOperation<Course> op = new TableOperation<Course>(dataSource, c)) {
				while (op.exists(k) && elapsed<timeout) {
					Thread.sleep(waitms);
					elapsed += waitms;
				}
			}
			assertNotEquals(timeout, elapsed);
						
		}
		tearDown();
	}

	public static Cache<Object, Record> createCache() {
		CachingProvider cachingProvider = Caching.getCachingProvider();
		CacheManager cacheManager = cachingProvider.getCacheManager();
		MutableConfiguration<Object, Record> config = new MutableConfiguration<Object, Record>().setTypes(Object.class, Record.class).setStatisticsEnabled(true);
		return cacheManager.createCache("simpleCache", config);
	}

	public static void setUp() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.setUp();
	}

	public static void tearDown() throws Exception {
		E10_WriteCSVDataIntoTheDatabase.tearDown();
	}	
	
}