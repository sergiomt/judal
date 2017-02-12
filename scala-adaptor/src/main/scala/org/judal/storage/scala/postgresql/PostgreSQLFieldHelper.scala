package org.judal.storage.scala.postgresql

import java.lang.Iterable
import java.util.Date
import java.lang.reflect.Constructor

import java.sql.Timestamp
import java.sql.SQLException

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

import com.knowgate.gis.LatLong
import com.knowgate.stringutils.Str

import org.judal.storage.Record
import org.judal.storage.FieldHelper

class PostgreSQLFieldHelper extends FieldHelper {
 
  private var cnr: Constructor[Iterable[java.util.Map.Entry[String, String]]] = null

	private def getArray[T](obj: AnyRef) : Array[T] = {
		if (null!=obj) {
		  if (obj.isInstanceOf[java.sql.Array]) {
		    val arr = obj.asInstanceOf[java.sql.Array]	  
			  try {
			    arr.getArray().asInstanceOf[Array[T]]
			  } catch  {
			    case sqle: SQLException => throw new ClassCastException(sqle.getMessage())
			  }
		  } else {
		    obj.asInstanceOf[Array[T]]
		  }
		} else {
			null
		}		  
	}

  def getIntegerArray(rec: Record, key: String) : Array[java.lang.Integer] = {
		val obj = rec.apply(key)
    if (obj.isInstanceOf[Array[Int]])
      obj.asInstanceOf[Array[Int]] map java.lang.Integer.valueOf
    else
		  getArray[Integer](obj)
  }

  def getLongArray(rec: Record, key: String) : Array[java.lang.Long] = {
		val obj = rec.apply(key)
    if (obj.isInstanceOf[Array[Long]])
      obj.asInstanceOf[Array[Long]] map java.lang.Long.valueOf
    else
		  getArray[java.lang.Long](obj)
  }

  def getFloatArray(rec: Record, key: String) : Array[java.lang.Float] = {
		val obj = rec.apply(key)
    if (obj.isInstanceOf[Array[Float]])
      obj.asInstanceOf[Array[Float]] map java.lang.Float.valueOf
    else
		  getArray[java.lang.Float](obj)
  }

  def getDoubleArray(rec: Record, key: String) : Array[java.lang.Double] = {
		val obj = rec.apply(key)
    if (obj.isInstanceOf[Array[Double]])
      obj.asInstanceOf[Array[Double]] map java.lang.Double.valueOf
    else
		  getArray[java.lang.Double](obj)
  }
  
	def getStringArray(rec: Record, key: String) : Array[String] = {
		val obj = rec.apply(key)
		if (null!=obj) {
		  if (obj.isInstanceOf[java.sql.Array]) {
		    val arr = obj.asInstanceOf[java.sql.Array]	  
			  try {
				  arr.getArray().asInstanceOf[Array[String]]
			  } catch {
			    case sqle: SQLException => throw new ClassCastException(sqle.getMessage())
			  }
		  } else {
			    obj.asInstanceOf[Array[String]]
			}
		} else {
			null
		}		  
	}

		def getDateArray(rec: Record, key: String) : Array[Date] = {
		val obj = rec.apply(key);
		if (null==obj) {
		  null
		} else if (obj.isInstanceOf[Array[Date]]) {
			obj.asInstanceOf[Array[Date]]
		} else if (obj.isInstanceOf[java.sql.Array]) {		     
			  try {
				  val arr = obj.asInstanceOf[java.sql.Array].getArray()
				  if (arr.isInstanceOf[Array[Date]]) {
				    arr.asInstanceOf[Array[Date]]
				  } else if (arr.isInstanceOf[Array[Timestamp]]) {
            arr.asInstanceOf[Array[Timestamp]] map (t => new Date(t.getTime()))
				  } else if (arr.isInstanceOf[Array[java.sql.Date]]) {
            arr.asInstanceOf[Array[java.sql.Date]] map (d => new Date(d.getYear(), d.getMonth(), d.getDate()))
				  } else {
		        throw new ClassCastException("PostgreSQLFieldHelper.getDateArray() cannot cast java.sql.Array to Array[Date]");
				  }
			  } catch {
			    case sqle: SQLException => throw new ClassCastException(sqle.getMessage())
			  }
		} else {
		  throw new ClassCastException("PostgreSQLFieldHelper.getDateArray() cannot cast from "+obj.getClass().getName()+" to Array[Date]");
		}
	}

	def getLatLong(rec: Record, key: String ) : LatLong = {
		if (rec.isNull(key)) {
			null
		} else if (rec.apply(key).isInstanceOf[LatLong]) {
			rec.apply(key).asInstanceOf[LatLong]
		} else {
			val latlng = rec.getString(key).split(" ")
			if (latlng.length!=2) throw new ArrayIndexOutOfBoundsException("PostgreSQLFieldHelper.getLatLong("+key+") bad geography format "+rec.getString(key))
			new LatLong(latlng(0).toFloat, latlng(1).toFloat)
		}
	}

	  /**
	   * <p>Get a part of an interval value</p>
	   * This function only works for PostgreSQL
	   * @param rec Record
	   * @param key String String Field Name
	   * @param part String Currently, only "days" is allowed as interval part
	   * @return int Number of days in the given interval
	   */
	def getIntervalPart(rec: Record, key: String, part: String) : Int = {
		if (part==null) throw new IllegalArgumentException("PostgreSQLFieldHelper.getIntervalPart() interval part to get cannot be null")
		if (!part.equalsIgnoreCase("days")) throw new IllegalArgumentException("PostgreSQLFieldHelper.getIntervalPart() interval part to get must be 'days'")
		val obj = rec.apply(key)
		if (obj==null) throw new NullPointerException("PostgreSQLFieldHelper.getIntervalPart() value of interval is null")
		val ti = obj.toString().toLowerCase()
		val mons = if (ti.indexOf("mons")<0) 0 else ti.indexOf("mons") + 4
		val days = ti.indexOf("days")
		if (days<0)
		  0
		else
		  Integer.parseInt(Str.removeChars(ti.substring(mons,days), " "))
	} // getIntervalPart

	/**
	 * <p>Get value of an HStore field<p>
	 * This method is only supported for PostgreSQL HStore fields
	 * @param key String Field name
	 * @return scala.collection.mutable.Map[String,String] or <b>null</b> if value of key field is <b>null</b>
	 */
	def getMap(rec: Record, key: String) : Map[String,String] = {
	  if (rec.isNull(key)) {
	    null
	  } else {
	    val vmp = new HashMap[String,String]
			val obj = rec.apply(key)
			if (cnr==null)
			  cnr = Class.forName("org.judal.jdbc.HStore").getConstructor(classOf[String]).asInstanceOf[Constructor[Iterable[java.util.Map.Entry[String, String]]]]
			val hst = cnr.newInstance(rec.toString())
			val itr = hst.iterator
			while (itr.hasNext) {
			  val ent = itr.next()
			  vmp.put(ent.getKey, ent.getValue)
			}
	    vmp
	  }
	}
  
}