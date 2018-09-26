package org.judal.storage.table;

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

import java.util.Set;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;

import javax.jdo.FetchGroup;
import javax.jdo.JDOUserException;
import javax.jdo.metadata.ColumnMetadata;

import org.judal.metadata.ColumnDef;

/**
* <p>Implementation of JDO FetchGroup.</p>
* @author Sergio Montoro Ten
* @version 1.0
*/
public class ColumnGroup implements FetchGroup, Iterable<String> {

	private Record rec;
	private String name;
	private boolean unmodifiable;
	private boolean postLoad;
	private HashMap<String,Integer> recursion;
	private LinkedHashSet<String> members;

	/**
	 * <p>Constructor.</p>
	 * @param columns String&hellip; Column names
	 */
	public ColumnGroup(String... columns) {
		rec = null;
		postLoad = false;
		recursion = null;
		unmodifiable = false;
		members = new LinkedHashSet<String>(columns.length*2+1);
		addMembers(columns);
	}

	/**
	 * <p>Constructor.</p>
	 * @param columns Iterable&lt;String&gt; Column names
	 */
	public ColumnGroup(Iterable<String> columns) {
		rec = null;
		postLoad = false;
		recursion = null;
		unmodifiable = false;
		members = new LinkedHashSet<String>();
		for (String column : columns)
			members.add(column);
	}

	/**
	 * <p>Constructor.</p>
	 * @param columns ColumnMetadata[]
	 */
	public ColumnGroup(ColumnMetadata[] columns) {
		rec = null;
		postLoad = false;
		recursion = null;
		unmodifiable = false;
		members = new LinkedHashSet<String>(columns.length*2+1);
		for (ColumnMetadata column : columns)
			members.add(column.getName());
	}

	/**
	 * <p>Constructor.</p>
	 * Create new ColumnGroup by cloning another one.
	 * @param record Record
	 */
	public ColumnGroup(Record record) {
		rec = record;
		postLoad = false;
		recursion = null;
		unmodifiable = false;
		members = new LinkedHashSet<String>();
		for (Object column : record.fetchGroup().getMembers())
			members.add((String) column);
	}

	/**
	 * <p>Add category of members.</p>
	 * If there is a Record And category is FetchGroup.ALL, FetchGroup.BASIC or FetchGroup.DEFAULT Then
	 * all the columns of the Record will be immediately added to the members list.
	 * @param category String
	 * @return FetchGroup <b>this</b>
	 * @throws JDOUserException If this ColumnGroup is marked as unmodifiable.
	 */
	@Override
	public FetchGroup addCategory(String category) throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		if (rec!=null)
			if (category.equals(FetchGroup.ALL) || category.equals(FetchGroup.BASIC) || category.equals(FetchGroup.DEFAULT))
				for (ColumnDef col : rec.columns())
					members.add(col.getName());
		return this;
	}

	/**
	 * <p>Add member.</p>
	 * @param memberName String
	 * @throws JDOUserException If this ColumnGroup is marked as unmodifiable.
	 */
	@Override
	public FetchGroup addMember(String memberName) {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		members.add(memberName);
		return this;
	}

	/**
	 * @param memberNames String&hellip; Column Names
	 */
	@Override
	public FetchGroup addMembers(String... memberNames) {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		for (String memberName : memberNames)
			members.add(memberName);
		return this;
	}

	/**
	 * @return Set&lt;String&gt;
	 */
	@Override
	public Set<String> getMembers() {
		return members;
	}

	/**
	 * @return String
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * @param name String
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * @return boolean
	 */
	@Override
	public boolean getPostLoad() {
		return postLoad;
	}

	/**
	 * @return int
	 */
	@Override
	public int getRecursionDepth(String memberName) {
		if (recursion==null)
			return 0;
		else if (recursion.containsKey(memberName))
			return recursion.get(memberName);
		else
			return 0;
	}

	/**
	 * @return Class
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public Class getType() {
		return rec==null ? null : rec.getClass();
	}

	/**
	 * @return boolean
	 */
	@Override
	public boolean isUnmodifiable() {
		return unmodifiable;
	}

	/**
	 * <p>Remove members category.</p>
	 * If category is FetchGroup.ALL Then all members will be cleared.
	 * Else if category is FetchGroup.BASIC or FetchGroup.DEFAULT Then
	 * all columns of the Record will be removed from members.
	 * @param category String Category name
	 * @return FetchGroup <b>this</b>
	 */
	@Override
	public FetchGroup removeCategory(String category) throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		if (category.equals(FetchGroup.ALL))
			members.clear();
		else if (rec!=null && (category.equals(FetchGroup.BASIC) || category.equals(FetchGroup.DEFAULT)))
			for (ColumnDef col : rec.columns())
				members.remove(col.getName());
		return this;
	}

	/**
	 * @param memberName String
	 * @return FetchGroup <b>this</b>
	 */
	@Override
	public FetchGroup removeMember(String memberName) throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		members.remove(memberName);		
		return this;
	}

	/**
	 * @param memberNames String&hellip;
	 * @return FetchGroup <b>this</b>
	 */
	@Override
	public FetchGroup removeMembers(String... memberNames) throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		for (String memberName : memberNames)
			members.remove(memberName);
		return this;
	}

	/**
	 * @param postLoad boolean
	 * @throws JDOUserException if this ColumnGroup is unmodifiable
	 * @return FetchGroup <b>this</b>
	 */
	@Override
	public FetchGroup setPostLoad(boolean postLoad) throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		this.postLoad = postLoad;
		return this;
	}

	@Override
	public FetchGroup setRecursionDepth(String memberName, int level) throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		if (recursion==null)
			recursion = new HashMap<String,Integer>(rec.size()*2);
		recursion.put(memberName, level);
		return this;
	}

	/**
	 * <p>Disallow any further modifications to this COlumnGroup.</p>
	 * @return FetchGroup <b>this</b>
	 */
	@Override
	public FetchGroup setUnmodifiable() {
		unmodifiable = true;
		return this;
	}

	/**
	 * @return Iterator&lt;String&gt; Iterator over the names of  the members
	 */
	@Override
	public Iterator<String> iterator() {
		return members.iterator();
	}

}
