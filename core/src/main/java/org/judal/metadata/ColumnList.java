package org.judal.metadata;

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

import java.util.ArrayList;

/**
* @author Sergio Montoro Ten
* @version 1.0
*/
public class ColumnList extends ArrayList<ColumnDef> {

private static final long serialVersionUID = 1l;
	
public ColumnList() { }

/**
 * Get column name by position
 * @param index int [0..size()-1]
 * @return DBColumn
 * @throws ArrayIndexOutOfBoundsException
 * @throws ClassCastException
 */
public String getColumnName(int index)
  throws ArrayIndexOutOfBoundsException, ClassCastException, NullPointerException {
  return get(index).getName();
}

/**
 * Get list of column names
 * @param sDelimiter String
 * @return String
 */
public String toString(String sDelimiter) {
  final int cCount = size();
  StringBuffer oBuffer = new StringBuffer(30*cCount);
  for (int c=0; c<cCount; c++) {
    if (c>0) oBuffer.append(sDelimiter);
    oBuffer.append(getColumnName(c));
  }
  return oBuffer.toString();
} // toString
}
