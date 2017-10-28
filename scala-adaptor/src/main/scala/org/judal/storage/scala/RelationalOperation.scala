package org.judal.storage.scala

import javax.jdo.FetchGroup
import javax.jdo.JDOUserException

import org.judal.storage.Param
import org.judal.storage.EngineFactory
import org.judal.storage.table.Record
import org.judal.storage.table.RecordSet

import org.judal.storage.relational.AbstractRelationalOperation
import org.judal.storage.relational.RelationalDataSource

import org.judal.storage.query.SortDirection.{ASC,DESC,same}

import scala.collection.JavaConverters._

class RelationalOperation[R >: Null <: Record](dataSource: RelationalDataSource , record: R) extends AbstractRelationalOperation[R](dataSource, record) {

	def this(record: R) = this(EngineFactory.getDefaultRelationalDataSource, record)

	def this(dataSource: RelationalDataSource ) = this(dataSource, null)

	override def fetch(maxrows: Int, offset: Int, keys: Param*) : Iterable[R] =
		getTable.fetch(getRecord.fetchGroup, maxrows, offset, keys: _*).asScala
		
	override def fetch(fetchGroup: FetchGroup , columnName: String, valueSearched: AnyRef) : Iterable[R] =
		getTable.fetch(fetchGroup, columnName, valueSearched).asScala

	override def fetchAsc(fetchGroup: FetchGroup , columnName: String, valueSearched: AnyRef, sortByColumn: String) : Iterable[R] = {
		val retval : RecordSet[R] = getTable.fetch(fetchGroup, columnName, valueSearched)
		retval.sort(sortByColumn)
		retval.asScala
	}
    		
	override def fetchDesc(fetchGroup: FetchGroup , columnName: String, valueSearched: AnyRef, sortByColumn: String) : Iterable[R] = {
		val retval : RecordSet[R] = getTable.fetch(fetchGroup, columnName, valueSearched)
		retval.sort(sortByColumn)
		retval.asScala
	}

	override def fetchFirst(fetchGroup: FetchGroup , columnName: String , valueSearched: Any, sortBy: String*) : R = {
		var retval: R = null
		var rst: RecordSet[R] = getTable.fetch(fetchGroup, columnName, valueSearched)
		if (rst.size()>0) {
		  if (sortBy!=null && sortBy.length==1)
		    rst.sort(sortBy(0))
		  else if (sortBy!=null && sortBy.length>1) {
			  if (same(ASC,sortBy(1)))
		      rst.sort(sortBy(0))
			  else if (same(DESC,sortBy(1)))
			    rst.sortDesc(sortBy(0))
			  else
				  throw new JDOUserException("Unrecognized sort direction " + sortBy(1))
		  }
		  retval = rst.get(0)
		}
		retval
	}	

}
