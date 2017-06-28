package org.judal.examples.scala.jdbc

import org.junit.Test

import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertEquals

import java.math.BigDecimal
import java.util.Calendar
import java.util.Date
import java.util.GregorianCalendar
import java.util.Map

import javax.cache.Cache
import javax.cache.CacheManager
import javax.cache.Caching
import javax.cache.configuration.MutableConfiguration
import javax.cache.spi.CachingProvider

import org.judal.storage.EngineFactory
import org.judal.storage.java.TableOperation
import org.judal.storage.queue.RecordQueueProducer
import org.judal.storage.relational.RelationalDataSource
import org.judal.storage.table.Record
import org.judal.storage.table.RecordManager

import org.judal.ramqueue.RAMQueueProducer
import org.judal.examples.java.model.pojo.Course
import org.judal.jdbc.JDBCEngine

import org.judal.Using._

/**
 * Example of how to use the RecordManager to write, read and delete records.
 * Write and delete operations are asynchronous using an in-memory queue.
 */
class E34_RecordManager {
	@Test
	def demo() : Unit = {
		
		E34_RecordManager.setUp()

		val cache = E34_RecordManager.createCache()

		val engineName = new JDBCEngine().name

		val dataSource = EngineFactory.getDefaultRelationalDataSource() 
		val properties = dataSource.getProperties()
		
		val producer = new RAMQueueProducer(engineName, properties)

		var manager : RecordManager = null
		
		using (manager) {
			manager = new RecordManager(dataSource, producer, cache, properties)
			val k = 1
			val c = new Course(dataSource)
			c.setId(k)
			c.setCode("AI07")
			c.setCourseName("Artificial Intelligence for Dummies")
			c.setPrice(new BigDecimal("1200"))
			val startd = new GregorianCalendar
			startd.setTime(new Date())
			c.setStartDate(startd)
			val endd = new GregorianCalendar
			endd.setTime(new Date(startd.getTimeInMillis()+(15l*86400000l)))
			c.setEndDate(endd)
			
			assertNotNull(c.getKey)
			
			manager.makePersistent(c)
			
			// Manager is asynchronous by default,
			// so wait for Record to be written
			// Active wait for demo purposes only
			// don't do while (!op.exists()) in production
			val waitms = 100l
			val timeout = 2000l
			var elapsed = 0l
			var op : TableOperation[Course] = null
			using (op) {
			  op = new TableOperation[Course](dataSource, c)
				while (!op.exists(k) && elapsed<timeout) {
					Thread.sleep(waitms)
					elapsed += waitms
				}
			}
			
			assertNotEquals(timeout, elapsed)
			
			val z = new Course()
			z.setId(k)
			
			manager.retrieve(z)
			
			assertEquals(c.getCode(), z.getCode())
			assertEquals(c.getStartDate(), z.getStartDate())
			
			manager.deletePersistent(c)
			
			elapsed = 0l
			using (op) {
			  op = new TableOperation[Course](dataSource, c)
				while (op.exists(k) && elapsed<timeout) {
					Thread.sleep(waitms)
					elapsed += waitms
				}
			}
			assertNotEquals(timeout, elapsed)
						
		}
		
		E34_RecordManager.tearDown()
	}
	
}


object E34_RecordManager {
  
	def createCache() : Cache[Object, Record] = {
		val cachingProvider = Caching.getCachingProvider()
		val cacheManager = cachingProvider.getCacheManager()
		val config : MutableConfiguration[Object, Record] = new MutableConfiguration[Object, Record]().setTypes(classOf[Object], classOf[Record]).setStatisticsEnabled(true)
		cacheManager.createCache("simpleCache", config)
	}

	def setUp() : Unit = {
		E10_WriteCSVDataIntoTheDatabase.setUp()
	}

	def tearDown() : Unit = {
		E10_WriteCSVDataIntoTheDatabase.tearDown()
	}	
  
}
