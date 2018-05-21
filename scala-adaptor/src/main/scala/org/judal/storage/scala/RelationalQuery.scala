package org.judal.storage.scala

import javax.jdo.JDOException

import org.judal.storage.EngineFactory
import org.judal.storage.StorageObjectFactory
import org.judal.storage.query.AbstractQuery
import org.judal.storage.query.relational.AbstractRelationalQuery
import org.judal.storage.table.Record
import org.judal.storage.table.RecordSet
import org.judal.storage.relational.RelationalDataSource

import scala.collection.JavaConverters._

class RelationalQuery[R >: Null <: Record](dts: RelationalDataSource , recClass: Class[R], alias: String) extends AbstractRelationalQuery[R](dts, recClass, alias) {

	def this(recClass: Class[R]) = this(EngineFactory.getDefaultRelationalDataSource, recClass, null);

	def this(recClass: Class[R], alias: String ) = this(EngineFactory.getDefaultRelationalDataSource(), recClass, alias)

	def this(dts: RelationalDataSource, recClass: Class[R]) = this(dts, recClass, null)

	def this(dts: RelationalDataSource, rec: R, alias: String ) = {
		this(dts, if (rec.getClass!=null) rec.getClass.asInstanceOf[Class[R]]  else null)
	  viw = dts.openRelationalView(rec)
	  if (alias!=null && alias.length()>0)
		  viw.getClass().getMethod("setAlias", classOf[String]).invoke(viw, alias)
		qry = viw.newQuery
		prd = qry.newPredicate
	}
	
	def this(rec: R, alias: String) = this(EngineFactory.getDefaultRelationalDataSource, rec, alias)

	def this(rec: R) = this(rec, null)
	
	def this(dts: RelationalDataSource, rec: R) = this(dts, rec, null);
  
  private def this() = this(null: RelationalDataSource, null: Class[R], null: String)

	override def clone() : RelationalQuery[R] = {
		val theClone = new RelationalQuery[R]()
		theClone.clone(this)
		theClone
	}

	override def fetch() : Iterable[R] = {
		qry.setFilter(prd)
		viw.fetch(qry).asScala
	}

	def fetchWithArray(params: (String,AnyRef)*) = {
		qry.declareParameters(params.map(p => p._1).mkString(","))
	  qry.executeWithArray(params.map(p => p._2):_*).asInstanceOf[RecordSet[R]].asScala
	}

	def fetchWithMap(params: scala.collection.mutable.LinkedHashMap[String,AnyRef]) = {
	  qry.declareParameters(params.keysIterator.mkString(","))
		qry.executeWithArray(params.values.toSeq:_*).asInstanceOf[RecordSet[R]].asScala
	}

	def fetchFirst() : R = {
		var rst : RecordSet[R] = null
	  qry.setFilter(prd)
		if (qry.getRangeFromIncl==0l && qry.getRangeToExcl==1l) {
		  rst = viw.fetch(qry)
		} else {
		  val q1 = clone
		  q1.setRange(0l, 1l)
		  rst = viw.fetch(q1.qry)
		}			
		  if (rst.isEmpty()) null else rst.get(0)
	}
	
}
