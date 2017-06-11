package org.judal.storage.scala

import javax.jdo.FetchGroup

import org.judal.storage.Param
import org.judal.storage.EngineFactory
import org.judal.storage.table.AbstractIndexableTableOperation
import org.judal.storage.table.Record
import org.judal.storage.table.RecordSet
import org.judal.storage.table.TableDataSource

import scala.collection.JavaConverters._

class IndexableTableOperation[R >: Null <: Record](dataSource: TableDataSource, record: R) extends AbstractIndexableTableOperation[R](dataSource: TableDataSource, record: R) {

	def this(record: R) = this(EngineFactory.getDefaultTableDataSource, record)

	def this(dataSource: TableDataSource ) = this(dataSource, null);

	override def fetch(maxrows: Int, offset: Int, keys: Param*) : Iterable[R] = 
		getTable.fetch(getRecord.fetchGroup, maxrows, offset, keys: _*).asScala

	override def fetch(fetchGroup: FetchGroup, columnName: String, valueSearched: AnyRef) =
		getTable.fetch(fetchGroup, columnName, valueSearched)

}