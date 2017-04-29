package org.judal.storage.scala

import org.judal.storage.table.Record

import scala.collection.mutable.Map

trait ScalaRecord extends Record with Map[String,AnyRef] {

}