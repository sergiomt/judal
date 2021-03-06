package org.judal.storage.scala

/**
  * © Copyright 2016 the original author.
  * This file is licensed under the Apache License version 2.0.
  * You may not use this file except in compliance with the license.
  * You may obtain a copy of the License at:
  * http://www.apache.org/licenses/LICENSE-2.0
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.
  */

import java.io.Serializable

import java.sql.Types;

import java.util.Arrays

import javax.jdo.JDOException

import org.judal.metadata.ViewDef
import org.judal.metadata.TableDef
import org.judal.metadata.ColumnDef
import org.judal.serialization.BytesConverter

import org.judal.storage.FieldHelper
import org.judal.storage.ConstraintsChecker
import org.judal.storage.EngineFactory
import org.judal.storage.table.impl.AbstractRecord
import org.judal.storage.table.TableDataSource

import scala.collection.Set
import scala.collection.mutable.DefaultEntry
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

import scala.collection.JavaConversions.asJavaCollection
import scala.collection.JavaConverters._

/**
  * Record interface implementation as a Scala mutable Map
  * @author Sergio Montoro Ten
  * @version 1.0
  */
class MapRecord(tableDefinition: ViewDef) extends AbstractRecord(tableDefinition: ViewDef) with ScalaRecord {

  /**
    * Column names are case insensitive, so specialize a HashMap to implement case insensitive lookups
    */
  class CaseInsensitiveValuesMap extends HashMap[String,AnyRef] {

    override def apply(key: String) : AnyRef = super.apply(key.toLowerCase)

    override def contains(key: String) : Boolean = super.contains(key.toLowerCase)

    override def isDefinedAt(key: String) : Boolean = super.isDefinedAt(key.toLowerCase)

    override def put (key: String, value: AnyRef) = super.put(key.toLowerCase, value)

    override def remove (key: String) = super.remove(key.toLowerCase)

    override def get (key: String) : Option[AnyRef] = super.get(key.toLowerCase)

  }

  var valuesMap = new CaseInsensitiveValuesMap
  var columnIndexes = Range(0,tableDefinition.getNumberOfColumns).map(c => c+1 -> tableDefinition.getColumns()(c).getName)(collection.breakOut): HashMap[Int,String]

  /**
    * Alternative constructor
    * @param tableDefinition ViewDef
    * @param fieldHelper FieldHelper
    * @param constraintsChecker ConstraintsChecker
    * @throws JDOException
    */
  @throws(classOf[JDOException])
  def this(tableDefinition: ViewDef, fieldHelper: FieldHelper, constraintsChecker: ConstraintsChecker) = {
    this(tableDefinition)
    setFieldHelper(fieldHelper);
    setConstraintsChecker(constraintsChecker);
  }

  /**
    * Alternative constructor
    * @param tableDefinition ViewDef
    * @param fieldHelper FieldHelper
    * @throws JDOException
    */
  @throws(classOf[JDOException])
  def this(tableDefinition: ViewDef, fieldHelper: FieldHelper) = {
    this(tableDefinition)
    setFieldHelper(fieldHelper);
  }

  /**
    * Alternative constructor
    * @param dataSource TableDataSource
    * @param tableName String
    * @throws JDOException
    */
  @throws(classOf[JDOException])
  def this(dataSource: TableDataSource, tableName: String) = {
    this(dataSource.getTableOrViewDef(tableName), dataSource.getFieldHelper)
  }

  /**
    * Alternative constructor using EngineFactory.getDefaultTableDataSource as DataSource
    * @param tableName String
    * @throws JDOException
    */
  @throws(classOf[JDOException])
  def this(tableName: String) = {
    this(EngineFactory.getDefaultTableDataSource, tableName)
  }

  /**
    * Alternative constructor
    * @param dataSource TableDataSource
    * @param tableName String
    * @param columnNames String*
    * @throws JDOException
    */
  @throws(classOf[JDOException])
  def this(dataSource: TableDataSource, tableName:String , columnNames:String*) = {
    this(new TableDef(tableName, MapRecordStatic.filterColumns(dataSource,  tableName, columnNames)), dataSource.getFieldHelper)
  }

  /**
    * Alternative constructor
    * @param tableDefinition ViewDef
    * @param constraintsChecker ConstraintsChecker
    * @throws JDOException
    */
  @throws(classOf[JDOException])
  def this(tableDefinition: ViewDef, constraintsChecker: ConstraintsChecker) = {
    this(tableDefinition)
    setConstraintsChecker(constraintsChecker);
  }

  def asEntries(): Array[java.util.Map.Entry[String,Object]] = valuesMap.asJava.entrySet.toArray(new Array[java.util.Map.Entry[String,Object]](valuesMap.size))

  def asMap(): java.util.Map[String,Object] = valuesMap.asJava

  /**
    * {@inheritDoc}
    */
  override def apply(columnName: String) : AnyRef = valuesMap.get(columnName).getOrElse(null)

  /**
    * {@inheritDoc}
    */
  override def clear() : Unit = valuesMap.clear()

  /**
    * {@inheritDoc}
    */
  override def contains(columnName: String) : Boolean = valuesMap.contains(columnName)

  /**
    * {@inheritDoc}
    */
  override def get (columnName: String) : Option[AnyRef] = valuesMap.get(columnName)

  /**
    * {@inheritDoc}
    */
  override def isDefinedAt(columnName: String) : Boolean = valuesMap.isDefinedAt(columnName)

  /**
    * {@inheritDoc}
    */
  override def keySet() : Set[String] = valuesMap.keySet

  /**
    * {@inheritDoc}
    */
  override def keys() = valuesMap.keys

  /**
    * <p>Set column value.</p>
    * @colpos int [1..columnCount()]
    * @obj AnyRef
    * @return Option[AnyRef]
    * @throws ArrayIndexOutOfBoundsException
    */
  override def put(colpos: Int, value: AnyRef) : Option[AnyRef] = {
    if (!columnIndexes.contains(colpos))
      throw new ArrayIndexOutOfBoundsException("Cannot find column at position " + colpos + " of " + columnIndexes.size +" columns")
    valuesMap.put(columnIndexes.get(colpos).get, value)
  }

  /**
    * {@inheritDoc}
    */
  override def put(colname: String, bytes: Array[Byte]) : Option[AnyRef] = {
    var retval : Array[Byte] = null
    val former = valuesMap.get(colname)
    if (former.isEmpty)
      retval = null
    else if (former.get.isInstanceOf[Array[Byte]])
      retval = Arrays.copyOf(former.asInstanceOf[Array[Byte]], former.asInstanceOf[Array[Byte]].length)
    else
      retval = BytesConverter.toBytes(former, Types.JAVA_OBJECT)
    valuesMap.put(colname, bytes);
    return Some(retval)
  }

  /**
    * {@inheritDoc}
    */
  override def remove(colname: String) : Option[AnyRef] = {
    if (valuesMap.contains(colname)) {
      val retval = valuesMap.get(colname)
      valuesMap.remove(colname)
      retval
    }  else {
      valuesMap.remove(colname)
      None
    }
  }

  /**
    * {@inheritDoc}
    */
  override def isEmpty() : Boolean = valuesMap.isEmpty

  /**
    * {@inheritDoc}
    */
  override def size() : Int = valuesMap.size

  /**
    * {@inheritDoc}
    */
  override def values() : Iterable[AnyRef] = valuesMap.values

  /**
    * {@inheritDoc}
    */
  override def getMap(key: String) : Map[String,String] = super.getMap(key).asInstanceOf[Map[String,String]]

  /**
    * {@inheritDoc}
    */
  @throws(classOf[JDOException])
  override def setValue(value: Serializable): Unit = {
    if (value.isInstanceOf[Map[String,AnyRef] @unchecked]) {
      valuesMap = new CaseInsensitiveValuesMap()
      columnIndexes = new HashMap[Int, String]()
      val valmap = value.asInstanceOf[Map[String,AnyRef]]
      val iter = valmap.keysIterator
      var n = 0
      while (iter.hasNext) {
        n += 1
        val key = iter.next
        valuesMap.put(key.asInstanceOf[String], valmap.get(key))
        columnIndexes.put(n, key.asInstanceOf[String])
      }
    } else {
      throw new ClassCastException("MapRecord.setValue() Cannot cast from "+value.getClass().getName()+" to java.util.Map");
    }
  }

  /**
    * {@inheritDoc}
    */
  @throws(classOf[JDOException])
  override def getValue() : scala.collection.mutable.Map[String,AnyRef] = valuesMap

  /**
    * <p>Set value at internal collection</p>
    * @param sKey Column Name
    * @param oObj Field Value
    * @throws NullPointerException If sKey is <b>null</b>
    * @throws IllegalArgumentException If no column with given name is not found at record columns list
    */
  override def put(colname: String, value: AnyRef): Option[AnyRef] = {
    val retval = valuesMap.get(colname)
    valuesMap.put(colname, value)
    retval
  }

  /**
    * {@inheritDoc}
    */
  override def +=(kv : (String, AnyRef)) = {
    valuesMap += kv
    this
  }

  /**
    * {@inheritDoc}
    */
  override def update(columnName: String, columnValue: AnyRef) : Unit = {
    valuesMap.update(columnName, columnValue)
  }

  /**
    * {@inheritDoc}
    */
  override def -=(colname: String) = {
    valuesMap.remove(colname)
    this
  }

  /**
    * {@inheritDoc}
    */
  def iterator: Iterator[(String, AnyRef)] = valuesMap.iterator

}

private object MapRecordStatic {

  def filterColumns(dataSource: TableDataSource, tableName: String , columnNames: Seq[String]) = {
    var retval : Array[ColumnDef] = null
    val vdef = dataSource.getTableOrViewDef(tableName)
    if (null==vdef) {
      retval = new Array[ColumnDef](columnNames.length)
      for (c <- 0 to columnNames.length-1)
        retval(c) = dataSource.createColumnDef(columnNames(c), c+1, Types.NULL, null)
    } else {
      retval = vdef.filterColumns(columnAliases(columnNames))
    }
    asJavaCollection(retval.toSeq)
  }

  def columnAliases(columnNames: Seq[String]) : Array[String] = {
    val colCount = columnNames.length
    val aliases = new Array[String](colCount)
    for (c <- 0 to colCount-1)
      aliases(c) = getColumnAlias(columnNames(c))
    return aliases;
  }

  def getColumnAlias(columnName: String) : String = {
    for (n <- 0 to columnName.length-6)
      if (columnName.charAt(n)==' ' &&
        (columnName.charAt(n+1)=='A' || columnName.charAt(n+1)=='a') &&
        (columnName.charAt(n+2)=='S' || columnName.charAt(n+2)=='s') &&
        columnName.charAt(n+3)==' ')
        return columnName.substring(n+4)
    return columnName
  }

}