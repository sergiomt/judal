package org.judal.storage.scala

import java.text.NumberFormat;
import java.text.DateFormat;
import java.text.Format;

import org.judal.storage.table.Record

trait ScalaRecord extends Record with scala.collection.mutable.Map[String,AnyRef] {
  
}