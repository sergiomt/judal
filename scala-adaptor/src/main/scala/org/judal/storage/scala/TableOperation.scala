package org.judal.storage.scala

import javax.jdo.FetchGroup

import org.judal.storage.EngineFactory
import org.judal.storage.table.AbstractTableOperation
import org.judal.storage.table.Record
import org.judal.storage.table.RecordSet
import org.judal.storage.table.TableDataSource

import scala.collection.JavaConverters._

class TableOperation[R >: Null <: Record](dataSource: TableDataSource , record: R) extends AbstractTableOperation[R](dataSource, record) {

	def this(record: R) = this(EngineFactory.getDefaultTableDataSource, record)

	def this(dataSource: TableDataSource) = this(dataSource, null)

	def fetch(fetchGroup: FetchGroup, columnName: String , valueSearched: AnyRef) : Iterable[R] =
		getTable.fetch(fetchGroup, columnName, valueSearched).asScala

}
