package org.judal.examples.scala.jdbc

import org.junit.Test
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertArrayEquals

import java.io.InputStream

import org.judal.examples.Resources
import org.judal.examples.java.model.map.Student
import org.judal.storage.java.TableOperation

import org.judal.Using._

/**
 * Example of how to write and read a LONGVARBINARY field
 */
class E28_BlobReadWrite {

	@Test
	def demo() : Unit = {
		
		val photoSize = 4832
		val photoBytes = new Array[Byte](photoSize)
		
		E28_BlobReadWrite.setUp()
		var op : TableOperation[Student] = null
		using (op) {
		  // Blobs are written and read using byte arrays as any other column
		  op = new TableOperation[Student](new Student())
			val s1 = op.load(new Integer(1))
			var photo : InputStream  = null
			using (photo) {
			  photo = Resources.getResourceAsStream("photo.jpg")
				assertNotNull(photo)
				photo.read(photoBytes)				
			}
			s1.put("photo", photoBytes)
			s1.store()			
		}

		using (op) {
		  op = new TableOperation[Student](new Student())
			val s1 = op.load(new Integer(1))
			assertArrayEquals(photoBytes, s1.getBytes("photo"))
		}

		E28_BlobReadWrite.tearDown()
	}
	
}


object E28_BlobReadWrite {
  
	def setUp() : Unit = {
		E10_WriteCSVDataIntoTheDatabase.setUp()
		E10_WriteCSVDataIntoTheDatabase.insertStudentsIntoDatabase()
	}

	def tearDown() : Unit = {
		E10_WriteCSVDataIntoTheDatabase.tearDown()
	}

}