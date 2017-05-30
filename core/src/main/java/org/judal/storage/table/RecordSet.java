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

import java.util.List;
import javax.jdo.Extent;

public interface RecordSet<R extends Record> extends Extent<R>, List<R> {
  
  /**
   * Return a list of all the Record elements for which the given Predicate evaluates to <b>true</b>.
   * @param predicate Object
   * @return List<R>
   */
  List<R> filter (final Object predicate);

  /**
   * Find the first row which value at columnName is equal to the given value
   * @param columnName String
   * @param value Object
   * @return Record instance or null if no row was found
   */
  R findFirst (final String columnName, final Object value);

  /**
   * Count of columns in this RecordSet
   * @return int
   */
  int getColumnCount();

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

  /**
   * Append the given RecordSet to this RecordSet
   * @param otherRecordSet RecordSet<R>
   * @throws ArrayIndexOutOfBoundsException
   * @throws NullPointerException
   */
  void addAll(RecordSet<R> otherRecordSet);
  
  /**
   * Number of Records in this RecordSet
   */
  int size();

  String toJson(final String name, final String identifier, final String label) throws ArrayIndexOutOfBoundsException;

}
