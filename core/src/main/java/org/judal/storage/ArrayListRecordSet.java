package org.judal.storage;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.FetchPlan;
import javax.jdo.PersistenceManager;

import org.judal.storage.Record;
import org.judal.storage.RecordColumnValueComparatorAsc;
import org.judal.storage.RecordColumnValueComparatorDesc;
import org.judal.storage.RecordSet;

/**
*
* <p>An array representing data read from a database table or view.</p>
* ArrayListRecordSet object is used for reading a collection of registers from the database and keep them in memory for immediate access.
* @author Sergio Montoro Ten
*/

public class ArrayListRecordSet<R extends Record> extends ArrayList<R> implements RecordSet<R> {

	private static final long serialVersionUID = 10000L;

	private Class<R> candidateClass;

	public ArrayListRecordSet(Class<R> candidateClass) {
		this.candidateClass = candidateClass;
	}

	public ArrayListRecordSet(Class<R> candidateClass, int capacity) {
		this.candidateClass = candidateClass;
		if (capacity>0)
			if (capacity==Integer.MAX_VALUE)
				ensureCapacity(capacity>128 ? 128 : capacity);
			else
				ensureCapacity(capacity>16384 ? 16384 : capacity);
	}

	@Override
	public List<R> filter(final Object predicate) {
		LinkedList<R> filtered = new LinkedList<R> ();
		try {
			Method m = predicate.getClass().getMethod("test", candidateClass);
			for (R record : this)
				if ((boolean) m.invoke(predicate, record))
					filtered.add(record);
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new NullPointerException("Cannot invoke method test() of predicate class "+predicate.getClass().getName());
		}
		return filtered;
	}

	@Override
	public R findFirst(String columnName, Object value) {
		for (R rec : this)
			if (rec.apply(columnName).equals(value))
				return rec;
		return null;
	}

	@Override
	public int getColumnCount() {
		return size()== 0 ? 0 : get(0).size();
	}

	@Override
	public void sort(String columnName) {
		sort(new RecordColumnValueComparatorAsc(columnName));
		
	}

	@Override
	public void sortDesc(String columnName) throws ArrayIndexOutOfBoundsException {
		sort(new RecordColumnValueComparatorDesc(columnName));		
	}

	@Override
	public void addAll(RecordSet<R> otherRecordSet) {
		ensureCapacity(size()+otherRecordSet.size());
		for (R rec : otherRecordSet)
			add(rec);
	}

	@Override
	public String toJson(String name, String identifier, String label) throws ArrayIndexOutOfBoundsException {
		return null;
	}

	@Override
	public void close(Iterator<R> iterator) {
		// Do nothing
	}

	@Override
	public void closeAll() {
		// Do nothing		
	}

	@Override
	public Class<R> getCandidateClass() {
		return candidateClass;
	}

	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		return null;
	}

	@Override
	public boolean hasSubclasses() {
		return false;
	}

} // ArrayListRecordSet

