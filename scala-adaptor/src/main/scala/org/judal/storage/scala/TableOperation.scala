package org.judal.storage.scala

import javax.jdo.FetchGroup
import javax.jdo.JDOUserException

import org.judal.storage.EngineFactory
import org.judal.storage.table.AbstractTableOperation
import org.judal.storage.table.Record
import org.judal.storage.table.RecordSet
import org.judal.storage.table.TableDataSource

import org.judal.storage.query.SortDirection.{ASC,DESC,same}

import scala.collection.JavaConverters._

class TableOperation[R >: Null <: Record](dataSource: TableDataSource , record: R) extends AbstractTableOperation[R](dataSource, record) {

	def this(record: R) = this(EngineFactory.getDefaultTableDataSource, record)

	def this(dataSource: TableDataSource) = this(dataSource, null)

	def fetch(fetchGroup: FetchGroup, columnName: String , valueSearched: AnyRef)  : Iterable[R] = {
		getTable.fetch(fetchGroup, columnName, valueSearched).asScala
	}
	
	def fetchAsc(fetchGroup: FetchGroup, columnName: String , valueSearched: AnyRef, sortByColumn: String) : Iterable[R]  = {
		val retval : RecordSet[R] = getTable.fetch(fetchGroup, columnName, valueSearched)
		retval.sort(sortByColumn)
		retval.asScala
	}
	
	def fetchDesc(fetchGroup: FetchGroup, columnName: String , valueSearched: AnyRef, sortByColumn: String) : Iterable[R]  = {
		val retval : RecordSet[R] = getTable.fetch(fetchGroup, columnName, valueSearched)
		retval.sortDesc(sortByColumn)
		retval.asScala
	}

	override def fetchFirst(fetchGroup: FetchGroup , columnName: String , valueSearched: Any, sortBy: String*) : R = {
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
		  val retval : R = rst.get(0)
		  setRecord(retval)
		} else {
		  setRecord(null)
		}
		getRecord()
	}
}