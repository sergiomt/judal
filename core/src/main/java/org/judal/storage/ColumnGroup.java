package org.judal.storage;

import java.util.Set;
import java.util.HashMap;
import java.util.LinkedHashSet;

import javax.jdo.FetchGroup;
import javax.jdo.JDOUserException;

import org.judal.metadata.ColumnDef;

/**
* @author Sergio Montoro Ten
* @version 1.0
*/
public class ColumnGroup implements FetchGroup {

	private static final long serialVersionUID = 1L;

	private Record rec;
	private boolean unmodifiable;
	private boolean postLoad;
	private HashMap<String,Integer> recursion;
	private LinkedHashSet<String> members;

	public ColumnGroup(String... columns) {
		rec = null;
		postLoad = false;
		recursion = null;		
		unmodifiable = false;
		members = new LinkedHashSet<String>();
		for (String column : columns)
			members.add(column);
	}

	public ColumnGroup(Record record) {
		rec = record;
		members = new LinkedHashSet<String>();
		postLoad = false;
		recursion = null;
		unmodifiable = false;
	}

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

	@Override
	public FetchGroup addMember(String memberName) {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		members.add(memberName);
		return this;
	}

	@Override
	public FetchGroup addMembers(String... memberNames) {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		for (String memberName : memberNames)
			members.add(memberName);
		return this;
	}

	@Override
	public Set<String> getMembers() {
		return members;
	}

	@Override
	public String getName() {
		return null;
	}

	@Override
	public boolean getPostLoad() {
		return postLoad;
	}

	@Override
	public int getRecursionDepth(String memberName) {
		if (recursion==null)
			return 0;
		else if (recursion.containsKey(memberName))
			return recursion.get(memberName);
		else
			return 0;
	}

	@Override
	public Class getType() {
		return rec==null ? null : rec.getClass();
	}

	@Override
	public boolean isUnmodifiable() {
		return unmodifiable;
	}

	@Override
	public FetchGroup removeCategory(String category) throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		if (category.equals(FetchGroup.ALL))
			members.clear();
		else if (category.equals(FetchGroup.BASIC) || category.equals(FetchGroup.DEFAULT))
			for (ColumnDef col : rec.columns())
				members.remove(col.getName());
		return this;
	}

	@Override
	public FetchGroup removeMember(String memberName) throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		members.remove(memberName);		
		return this;
	}

	@Override
	public FetchGroup removeMembers(String... memberNames) throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		for (String memberName : memberNames)
			members.remove(memberName);
		return this;
	}

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

	@Override
	public FetchGroup setUnmodifiable() throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException("FetchGroup is unmodifiable");
		unmodifiable = true;
		return this;
	}

}
