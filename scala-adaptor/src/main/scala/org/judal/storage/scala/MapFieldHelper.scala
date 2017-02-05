package org.judal.storage.scala

import java.lang.Iterable

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

object MapFieldHelper {
 
	/**
	 * <p>Get value of an HStore field<p>
	 * This method is only supported for PostgreSQL HStore fields
	 */
	def getMap(rec: ScalaRecord, key: String) : Map[String,String] = {
	  if (rec.isNull(key)) {
	    null
	  } else {
	    val vmp = new HashMap[String,String]
			val obj = rec.apply(key)
			val cls = Class.forName("org.judal.jdbc.HStore")
			val hst = cls.getConstructor(classOf[String]).newInstance(rec.toString())
			val itr = hst.asInstanceOf[Iterable[java.util.Map.Entry[String, String]]].iterator
			while (itr.hasNext) {
			  val ent = itr.next()
			  vmp.put(ent.getKey, ent.getValue)
			}
	    vmp
	  }
	}
  
}