package org.judal.jdbc.test;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Map;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import javax.jdo.JDOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.judal.storage.EngineFactory;
import org.judal.storage.Record;
import org.judal.storage.RecordManager;
import org.judal.storage.TableDataSource;
import org.judal.storage.java.test.AbstractRecordManagerTest;
import org.judal.storage.java.test.ArrayRecord2;
import org.judal.ramqueue.RAMQueueProducer;

import org.judal.jdbc.JDBCEngine;

public class TestRecordManager extends AbstractRecordManagerTest {

	private static JDBCEngine jdbc;
	private static Map<String,String> properties;
	private static Cache<Object, Record> cache;

	@BeforeClass
	public static void init() throws ClassNotFoundException, JDOException, IOException, IllegalStateException, InstantiationException {
		properties = new TestJDBC().getTestProperties();
		jdbc = new JDBCEngine();
		EngineFactory.registerEngine(jdbc.name(), jdbc.getClass().getName());
		recordClass2 = ArrayRecord2.class;
		TableDataSource dts = jdbc.getDataSource(properties);
		createTable2(dts);
		dts.close();
		CachingProvider cachingProvider = Caching.getCachingProvider();
		CacheManager cacheManager = cachingProvider.getCacheManager();
		MutableConfiguration<Object, Record> config = new MutableConfiguration<Object, Record>().setTypes(Object.class, Record.class).setStatisticsEnabled(true);
		 cache = cacheManager.createCache("simpleCache", config);
	}

	@AfterClass
	public static void cleanup() throws JDOException {
		TableDataSource dts = jdbc.getDataSource(properties);
		dts.dropTable(ArrayRecord2.tableName, false);
		dts.close();
	}
	
	@Test
	public void test01Manager() throws InstantiationException, IllegalAccessException, InterruptedException, RemoteException {
		TableDataSource dts = jdbc.getDataSource(properties);
		RAMQueueProducer que = new RAMQueueProducer(jdbc.name(), properties);
		man = new RecordManager(dts, que, cache, properties);
		super.test01Manager();
	}

	// @Test(expected=JDOUserException.class)
	// public void test02Cache() throws JDOUserException {
	//	super.test02Cache();
	// }	
	
}
