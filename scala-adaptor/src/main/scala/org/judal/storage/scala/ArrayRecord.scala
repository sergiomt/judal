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

import java.sql.Types

import java.util.Arrays

import javax.jdo.JDOException
import javax.jdo.JDOUserException

import org.judal.metadata.ColumnDef
import org.judal.metadata.TableDef

import org.judal.serialization.BytesConverter

import org.judal.storage.AbstractRecord
import org.judal.storage.TableDataSource

import com.knowgate.typeutils.TypeResolver

import scala.collection.JavaConversions.asJavaCollection

import scala.collection.mutable.DefaultEntry
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

/**
 * Record interface implementation as an array indexed by column name
 * @author Sergio Montoro Ten
 * @version 1.0
 */
class ArrayRecord(tableDefinition: TableDef) extends AbstractRecord(tableDefinition: TableDef) with Map[String,AnyRef] {

	var valuesArray = new Array[AnyRef](getTableDef.getNumberOfColumns)
	var columnsMap : HashMap[String,Int] = null
	clear()

	/**
	 * Alternative constructor
	 * @param dataSource TableDataSource
	 * @param tableName String
	 * @throws JDOException 
	 */
	@throws(classOf[JDOException])
	def this(dataSource: TableDataSource, tableName: String) = {
		this(dataSource.getTableDef(tableName))
	}

	/**
	 * Alternative constructor
	 * @param dataSource TableDataSource
	 * @param tableName String
	 * @param columnNames String*
	 * @throws JDOException 
	 */	
	@throws(classOf[JDOException])
	def this(dataSource: TableDataSource, tableName: String, columnNames:String* ) {
		this(new TableDef(tableName, asJavaCollection(dataSource.getTableDef(tableName).filterColumns(columnNames.map(c => AbstractRecord.getColumnAlias(c)).toArray[String]).toSeq)))
		var pos = 0
		for (columnName <- columnNames) {
			val columnAlias = AbstractRecord.getColumnAlias(columnName)
			val cdef = getTableDef.getColumnByName(columnAlias)
			if (cdef==null)
			  throw new JDOUserException("Column "+columnAlias+" not found at "+getTableName)
			pos += 1
			columnsMap.put(columnAlias, pos)
		}
	}

	/**
	 * Get value for column
	 * @param columnName String Column Name
	 * @return AnyRef Column value or <b>null</b> if this record contains no column with such name
	 */
	override def apply(columnName: String) : AnyRef = {
		val index = getColumnIndex(columnName)
		if (index.equals(-1))
		  null
		else
		  valuesArray(index-1)
	}

	override def clear() : Unit = {
	  Arrays.fill(valuesArray, null)
	}
	
	override def contains(columnName: String) : Boolean = apply(columnName)!=null

	/**
	 * Get optional value for column
	 * @param columnName String Column Name
	 * @return Option[AnyRef]
	 */
	override def get(columnName: String) : Option[AnyRef] = Option(apply(columnName))
	
	override def keySet() : Set[String] = getTableDef.getColumns.toList.map(c => c.getName).toSet

	override def isDefinedAt(columnName: String) : Boolean = keySet().contains(columnName)
	
	override def keys() : Iterable[String] = getTableDef.getColumns.toList.map(c => c.getName)

	/**
	 * @return Int [1..size()]
	 */
	def getColumnIndex(columnName: String) : Int =
	  if (null==columnsMap) getTableDef.getColumnIndex(columnName) else columnsMap.get(columnName).get
	
	/**
	 * @return AnyRef if this instance isBucket() or Array[AnyRef] otherwise
	 */
	@throws(classOf[JDOException])
	override def getValue() : AnyRef = {
		val isBucket = (valuesArray.length==2)
		if (isBucket) {
			val pkIndex = getColumnIndex(getTableDef.getPrimaryKeyMetadata.getColumn)
			val valIndex = (if (pkIndex.equals(1)) 2 else 1) - 1
			valuesArray(valIndex)
		} else {
			valuesArray
		}
	}

	/**
	 * Set column values from a given array.
	 * @param newvalue Array[AnyRef]
	 * @throws JDOException
	 */
	@throws(classOf[JDOException])
	override def setValue(newvalue: Serializable) : Unit = {
		if (newvalue==null) {
			valuesArray = new Array[AnyRef](getTableDef.getNumberOfColumns)
		} else {
			val pkIndex = getColumnIndex(getTableDef.getPrimaryKeyMetadata.getColumn)
			val valIndex = (if (pkIndex==1) 2 else 1) - 1
			val isBucket = (valuesArray.length==2)
			if (newvalue.getClass.isArray) {
			  if (isBucket && (newvalue.isInstanceOf[Array[Byte]] || newvalue.isInstanceOf[Array[Char]])) {
					valuesArray(valIndex) = newvalue.asInstanceOf[AnyRef]
				} else {
					if (newvalue.isInstanceOf[Array[AnyRef]]) {
						val newvalues = newvalue.asInstanceOf[Array[AnyRef]]
						valuesArray = new Array[AnyRef](getTableDef.getNumberOfColumns)
						if (valuesArray.length < newvalues.length)
							throw new JDOException("Supplied more values ("+String.valueOf(newvalues.length)+") than columns at table "+getTableDef.getName+"("+String.valueOf(valuesArray.length)+")")
						for (k <- Range(0,newvalues.length))
							valuesArray(k) = newvalues(k)
					} else {
						throw new JDOException("Type mismatch. AnyRef[] expected but got "+newvalue.getClass.getName+" for "+String.valueOf(valuesArray.length)+" values");
					}
				}
			} else {
				if (isBucket)
					valuesArray(valIndex) = newvalue.asInstanceOf[AnyRef]
				else
					throw new JDOException("Type mismatch. Array expected but got "+newvalue.getClass.getName);
			}
		}
	}

	override def size() : Int = valuesArray.length

	/**
	 * @return Boolean <b>true</b> if every column value is <b>null</b>
	 */
	override def isEmpty() : Boolean = values.forall(o => o==null)

	/**
	 * Remove value for column
	 * @param colname String Column Name
	 */
	override def remove(colname: String) : Option[AnyRef] = {
		var retval : AnyRef = null
		val index = getColumnIndex(colname)
		if (!index.equals(-1)) {
			retval = valuesArray(index-1)
			valuesArray(index-1) = null
		}
		Option(retval)
	}

	/**
	 * 
	 * @colpos int [1..columnCount()]
	 * @obj AnyRef
	 * @throws ArrayIndexOutOfBoundsException
	 */
	override def put(colpos: Int, obj: AnyRef) : AnyRef = {
		val retval = valuesArray(colpos-1)
		valuesArray(colpos-1) = obj
		retval
	}

	override def put(colname: String, bytes: Array[Byte]) : AnyRef = {
		var retval : Array[Byte] = null
		val index = getColumnIndex(colname) - 1
		if (null==valuesArray(index)) {
			retval = null
		} else if ( valuesArray(index).isInstanceOf[Array[Byte]] ) {
			val byteArray = valuesArray(index).asInstanceOf[Array[Byte]]
		  retval = Arrays.copyOf(byteArray, byteArray.length)
		} else {
			retval = BytesConverter.toBytes(valuesArray(index), Types.JAVA_OBJECT)
		}
		valuesArray(index) = bytes
		retval
	}
	
	override def values() : Iterable[AnyRef] = values.toSeq

	/**
	 * <p>Set value at internal collection</p>
	 * @param sKey String Column Name
	 * @param oObj AnyRef Field Value
	 * @return Option[AnyRef]
	 * @throws NullPointerException If sKey is <b>null</b>
	 * @throws IllegalArgumentException If no column with given name is not found at record columns list
	 */
	@throws(classOf[NullPointerException])
	@throws(classOf[IllegalArgumentException])
	override def put(sKey: String, oObj: AnyRef) : Option[AnyRef] = {

		if (sKey==null)
			throw new NullPointerException("ArrayRecord.put(String,AnyRef) field name cannot be null");

		val index = getColumnIndex(sKey) - 1
		if (index<0)
			throw new IllegalArgumentException("Column "+sKey+" not found at table "+getTableDef.getName)

		val retval = Option(valuesArray(index))

		if (null==oObj) {
			valuesArray(index) = null;
		} else if (TypeResolver.isOfStandardType(oObj)) {
			valuesArray(index) = oObj
		} else {
			val oCol = getTableDef.getColumnByName(sKey)
			if (oCol!=null) {
				valuesArray(index) = if (oCol.isOfBinaryType()) getBinaryData(sKey, oObj) else oObj
			} else {
				throw new IllegalArgumentException("Column "+sKey+" does not exist at "+getTableDef.getName)
			}
		}
		retval
	}
	
	override def iterator : Iterator[(String,AnyRef)] = columnsMap.map(c => (c._1, valuesArray(c._2-1))).iterator

	/**
	 * Update the value of a column
	 * @param columnName String
	 * @param columnValue AnyRef
	 * @throws NullPointerException if columnName is <b>null</b>
	 * @throws IllegalArgumentException if columnName is not the name of any column
	 */
  override def update(columnName: String, columnValue: AnyRef) : Unit = {
		if (columnName==null)
			throw new NullPointerException("ArrayRecord.put(String,Object) column name cannot be null");

		val index = getColumnIndex(columnName) - 1
		if (index<0)
			throw new IllegalArgumentException("Column "+columnName+" not found at table "+getTableDef.getName)

		if (null==columnValue) {
			valuesArray(index) = null
		} else if (TypeResolver.isOfStandardType(columnValue)) {
			valuesArray(index) = columnValue
		} else {
			val oCol = getTableDef.getColumnByName(columnName)
			if (oCol!=null) {
				valuesArray(index) = if (oCol.isOfBinaryType()) getBinaryData(columnName, columnValue) else columnValue
			} else {
				throw new IllegalArgumentException("Column "+columnName+" does not exist at "+getTableDef.getName)
			}
		}
  }
	
  override def +=(kv : (String, AnyRef)) = {
    update(kv._1, kv._2)
		this
	}
	
  override def -=(colname: String) = {
		val index = getColumnIndex(colname)
		if (!index.equals(-1))
			valuesArray(index-1) = null
		this
  }

}
