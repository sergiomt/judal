package org.judal.storage.table;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.io.IOException;

import java.text.DateFormat;
import java.text.Format;
import java.text.NumberFormat;

public interface RecordSet<R extends Record> extends Iterable<R> {
  
  /**
   * Add a Record to the end of this RecordSet.
   * @param record &lt;R extends Record&gt;
   * @return boolean
   */
  boolean add(final R record);

  /**
   * Append the Records of the given RecordSet to the end of this RecordSet
   * @param otherRecordSet RecordSet<R>
   * @throws ArrayIndexOutOfBoundsException
   * @throws NullPointerException
   */
  void addAll(RecordSet<R> otherRecordSet);
  
  /**
   * Return the Record in a given position.
   * @param index int Position starting at 0.
   * @return &lt;R extends Record&gt;
   */
  R get (final int index);

  /**
   * Return a list of all the Record elements for which the given Predicate evaluates to <b>true</b>.
   * @param predicate Object
   * @return Iterable<R>
   */
  Iterable<R> filter (final Object predicate);

  /**
   * Find the first row which value at columnName is equal to the given value
   * @param columnName String
   * @param value Object
   * @return Record instance or null if no row was found
   */
  Object findFirst (final String columnName, final Object value);

  
  @SuppressWarnings("rawtypes")
  /**
   * Get the class of Record instance contained in this RecordSet
   * @return Class
   */
  Class getCandidateClass();

  /**
   * Get number of Records in this RecordSet
   * @return int
   */
  int size();
  
  /**
   * Sort the RecordSet by the given column in ascending order
   * @param columnName String
   * @throws ArrayIndexOutOfBoundsException If no column has the given name
   */
  void sort(final String columnName) throws ArrayIndexOutOfBoundsException;

  /**
   * Sort the RecordSet by the given column in descending order
   * @param columnName String
   * @throws ArrayIndexOutOfBoundsException If no column has the given name
   */
  void sortDesc(final String columnName) throws ArrayIndexOutOfBoundsException;

  String toJson(final String name, final String identifier, final String label) throws IOException;

  String toXML(String identSpaces, DateFormat dateFormat, NumberFormat decimalFormat, Format textFormat) throws IOException;
  
}
