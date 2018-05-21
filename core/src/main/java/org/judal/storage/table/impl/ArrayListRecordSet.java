package org.judal.storage.table.impl;

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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.text.DateFormat;
import java.text.Format;
import java.text.NumberFormat;

import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

import org.judal.storage.table.comparators.RecordColumnValueComparatorAsc;
import org.judal.storage.table.comparators.RecordColumnValueComparatorDesc;

/**
*
* <p>An array representing data read from a database table or view.</p>
* ArrayListRecordSet object is used for reading a collection of registers from the database and keep them in memory for immediate access.
* @author Sergio Montoro Ten
* @version 1.0
* @param &lt;R extends Record&gt;
*/
public class ArrayListRecordSet<R extends Record> extends ArrayList<R> implements RecordSet<R> {

	private static final long serialVersionUID = 10000L;

	public static int MAX_DEFAULT_CAPACITY = 16384;

	private Class<R> candidateClass;

	/**
	 * <p>Construct ArrayListRecordSet for Records of class R and initial capacity for 10 results.</p>
	 * @param candidateClass Class&lt;R&gt;
	 */
	public ArrayListRecordSet(Class<R> candidateClass) {
		this.candidateClass = candidateClass;
	}

	/**
	 * <p>Construct ArrayListRecordSet for Records of class R and initial capacity for 10 results.</p>
	 * @param candidateClass Class&lt;R&gt;
	 * @param capacity int Initial capacity to be allocated for results
	 */
	public ArrayListRecordSet(Class<R> candidateClass, int capacity) {
		this.candidateClass = candidateClass;
		if (capacity>0)
			if (capacity>MAX_DEFAULT_CAPACITY)
				ensureCapacity(MAX_DEFAULT_CAPACITY);
	}

	/**
	 * <p>Construct ArrayListRecordSet for Records of class R and initial capacity for 10 results.</p>
	 * @param candidateClass Class&lt;R&gt;
	 * @param capacity Integer Initial capacity to be allocated for results
	 */
	public ArrayListRecordSet(Class<R> candidateClass, Integer capacity) {
		this(candidateClass, capacity.intValue());
	}

	/**
	 * <p>Get sublist of results that match a given predicate.</p>
	 * @param predicate Object Instance of a class containing a public method {@code boolean test(R)}
	 * @return List&lt;R&gt;
	 */
	@Override
	public List<R> filter(final Object predicate) {
		LinkedList<R> filtered = new LinkedList<R> ();
		if (null==getCandidateClass())
			throw new NullPointerException("ArrayListRecordSet.filter(9 cannot be invoked if ResuluSet candidate class is null");
		try {
			Method m = predicate.getClass().getMethod("test", getCandidateClass());
			for (R record : this)
				if ((boolean) m.invoke(predicate, record))
					filtered.add(record);
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new NullPointerException("Cannot invoke method test() of predicate class " + predicate.getClass().getName() + "<" + getCandidateClass().getName() + "> " + e.getClass().getName() + " " +e.getMessage());
		}
		return filtered;
	}

	/**
	 * <p>Find first result in this RecordSet which columnName contains the given value.</p>
	 * @param columnName String Column Name
	 * @param value Object value May be null but do not use Scala Option instances as input value here.
	 * @return R or <b>null</b> if no Record is found matching the search criteria
	 */
	@Override
	public R findFirst(String columnName, Object value) {
		if (value==null) {
			for (R r1 : this)
				if (r1.isNull(columnName))
					return r1;
		} else {
			for (R r2 : this)
				if (value.equals(r2.apply(columnName)))
					return r2;
		}
		return null;
	}

	/**
	 * <p>Sort in ascending order on columnName
	 * @param columnName String
	 * @see org.judal.storage.table.comparators.RecordColumnValueComparatorAsc
	 */
	@Override
	public void sort(String columnName) {
		sort(new RecordColumnValueComparatorAsc(columnName));
		
	}

	/**
	 * <p>Sort in descending order on columnName
	 * @param columnName String
	 * @see org.judal.storage.table.comparators.RecordColumnValueComparatorDesc
	 */
	@Override
	public void sortDesc(String columnName) throws ArrayIndexOutOfBoundsException {
		sort(new RecordColumnValueComparatorDesc(columnName));		
	}

	/**
	 * <p>Append another RecordSet to this one.</p>
	 * @param otherRecordSet RecordSet&lt;R&gt;
	 */
	@Override
	public void addAll(RecordSet<R> otherRecordSet) {
		final int newRecCount = otherRecordSet.size();
		ensureCapacity(size()+newRecCount);
		for (R rec : otherRecordSet)
			add(rec);
	}

	/**
	 * <p>Get representation of this RecordSet as a JSON array.</p>
	 * @return String
	 * @throws IOException
	 */
	@Override
	public String toJSON() throws IOException {
		StringBuilder output = new StringBuilder();
		output.append("[");
		for (Record r : this)
			output.append(r.toJSON()).append(",");
		if (output.length()>1) output.setLength(output.length()-1);
		output.append("]");
		return output.toString();
	}

	/**
	  * Write RecordSet as XML.
	  * @param identSpaces String Number of indentation spaces at the beginning of each line.
	  * @param dateFormat DateFormat Date format.
	  * @param decimalFormat NumberFormat Decimal format.
	  * @param textFormat Format. Custom formatter for text fields. May be used to encode text as HTML or similar transformations.
	  * @return String
	*/
	public String toXML(String identSpaces, DateFormat dateFormat, NumberFormat decimalFormat, Format textFormat) throws IOException {
		String className = candidateClass.getName();
		int dot = className.lastIndexOf('.');
		if (dot>0)
			className = className.substring(dot+1);
		final String ident = identSpaces==null ? "" : identSpaces;
		if (size()==0) {
			return ident + "<RecordSet of=\""+className+"\" />";
		} else {
			StringBuilder output = new StringBuilder();
			output.append(ident).append("<RecordSet of=\"").append(className).append("\" />").append(identSpaces==null ? "" : "\n");
			for (Record r : this)
				output.append(r.toXML(identSpaces, dateFormat, decimalFormat, textFormat)).append(identSpaces==null ? "" : "\n");
			output.append(ident).append("</RecordSet>");
			return output.toString();
		}
	}

	/**
	 * @return Class&lt;R&gt;
	 */
	@Override
	public Class<R> getCandidateClass() {
		return candidateClass;
	}

} // ArrayListRecordSet

