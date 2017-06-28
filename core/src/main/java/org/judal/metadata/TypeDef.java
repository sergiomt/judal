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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;
import javax.jdo.JDOUnsupportedOptionException;

import javax.jdo.metadata.ColumnMetadata;
import javax.jdo.metadata.DatastoreIdentityMetadata;
import javax.jdo.metadata.ForeignKeyMetadata;
import javax.jdo.metadata.IndexMetadata;
import javax.jdo.metadata.InheritanceMetadata;
import javax.jdo.metadata.MemberMetadata;
import javax.jdo.metadata.PropertyMetadata;
import javax.jdo.metadata.QueryMetadata;
import javax.jdo.metadata.TypeMetadata;
import javax.jdo.metadata.UniqueMetadata;
import javax.jdo.metadata.VersionMetadata;

import com.knowgate.debug.DebugFile;

import javax.jdo.annotations.IdentityType;

/**
 * Implementation of JDO TypeMetadata interface
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class TypeDef extends BaseDef implements TypeMetadata {

	private static final long serialVersionUID = 10000l;

	@SuppressWarnings("serial")
	private class CaseInsensitiveColumnMap extends HashMap<String,Integer> {
		@Override
		public Integer put (String key, Integer position) {
			return super.put(key.toLowerCase(), position);
		}
		@Override
		public Integer remove (Object key) {
			return super.remove(((String) key).toLowerCase());
		}
		@Override
		public Integer get (Object key) {
			return super.get(((String) key).toLowerCase());
		}
		@Override
		public boolean containsKey (Object key) {
			return super.containsKey(((String) key).toLowerCase());
		}
	}

	private String schema;
	private String catalog;
	private String objectIdClass;
	private boolean detachable;
	private boolean serializeRead;
	private boolean requiresExtent;
	private IdentityType identityType;
	private DatastoreIdentityMetadata identityMeta;
	private List<MemberGroupDef> fetchGroups;
	private HashMap<String,QueryDef> queries;
	private CaseInsensitiveColumnMap positions;

	/**
	 * <p>Constructor.</p>
	 */
	public TypeDef() {
		queries = new HashMap<String,QueryDef>();
		positions = new CaseInsensitiveColumnMap();
		fetchGroups = new LinkedList<MemberGroupDef>();
		identityMeta = null;
		identityType = IdentityType.DATASTORE;
		serializeRead = false;
		requiresExtent = false;
		schema = null;
		catalog = null;
	}

	/**
	 * <p>Create TypeDef by cloning another one.</p>
	 */
	public TypeDef(TypeDef source) {
		super(source);
		schema = source.schema;
		catalog = source.catalog;
		queries = new HashMap<String,QueryDef>();
		for (Map.Entry<String,QueryDef> query : source.queries.entrySet())
			queries.put(query.getKey(), query.getValue());
		positions = new CaseInsensitiveColumnMap();
		for (Map.Entry<String,Integer> position : source.positions.entrySet())
			positions.put(position.getKey(), position.getValue());
		identityMeta = source.identityMeta;
		identityType = source.identityType;
		serializeRead = source.serializeRead;
		requiresExtent = source.requiresExtent;
	}

	/**
	 * @return TypeDef Clone of <b>this</b>
	 */
	@Override
	public TypeDef clone() {
		return new TypeDef(this);
	}
	
	protected void ensureColumnsCapacity(int minCapacity) {
		columns.ensureCapacity(minCapacity);
	}

	/**
	 * @return String Catalog name or empty String or <b>null</b>
	 */
	@Override
	public String getCatalog() {
		return catalog;
	}


	/**
	 * @return Column names separated by commas. If column count is zero then empty String is returned.
	 */
	public String getColumnsStr() {
		if (getNumberOfColumns()==0) {
			return "";
		} else {
			final int ncols = getNumberOfColumns();
			StringBuilder builder = new StringBuilder(ncols*30);
			builder.append(columns.get(0).getName());
			for (int c=1; c<ncols; c++)
				builder.append(',').append(columns.get(c).getName());
			return builder.toString();
		}
	} // getColumnsStr

	/**
	 * <p>Get column by name.</p>
	 * Name matching is case insensitive.
	 * @param columnName String
	 * @return ColumnDef
	 * @throws ArrayIndexOutOfBoundsException If no column with the given name is found.
	 */
	public ColumnDef getColumnByName(String columnName) throws ArrayIndexOutOfBoundsException {
		final int ncols = columns.size();
		if (ncols!=positions.size()) {
			positions.clear();
			for (int p=0; p<columns.size(); p++)
				positions.put(columns.get(p).getName(), p);
		}
		Integer npos = positions.get(columnName);
		if (null==npos)
			throw new ArrayIndexOutOfBoundsException("Column "+columnName+" not found at "+getName());
		return columns.get(npos);
	}

	/**
	 * Column position at the table
	 * @param columnName String
	 * @return int [1..columnCount()] or -1 if no column with such name exists
	 */
	public int getColumnIndex(String columnName) {
		ColumnDef cdef = null;
		int colIndex;
		try {
			cdef = getColumnByName(columnName);
			colIndex = cdef.getPosition();
		} catch (ArrayIndexOutOfBoundsException notfound) {
			colIndex = -1;
		}
		return colIndex;
	}
	
	/**
	 * @return DatastoreIdentityMetadata
	 */
	@Override
	public DatastoreIdentityMetadata getDatastoreIdentityMetadata() {
		return identityMeta;
	}

	/**
	 * @return boolean
	 */
	@Override
	public boolean getDetachable() {
		return detachable;
	}

	/**
	 * @return boolean
	 */
	@Override
	public Boolean getEmbeddedOnly() {
		return embeddedOnly;
	}

	/**
	 * <p>Get array of known fetch groups applicable to this TypeDef.</p>
	 * @return MemberGroupDef[]
	 */
	@Override
	public MemberGroupDef[] getFetchGroups() {
		return fetchGroups.toArray(new MemberGroupDef[fetchGroups.size()]);
	}

	/**
	 * <p>Get fetch group by name.</p>
	 * @param groupName String
	 * @return MemberGroupDef or <b>null</b> if no fetch group is found with the given name.
	 * @throws NullPointerException If groupName is <b>null</b>.
	 */
	public MemberGroupDef getFetchGroup(String groupName) throws NullPointerException {
		if (null==groupName)
			throw new NullPointerException("TypeDef.getFetchGroup() group name cannot be null");
		for (MemberGroupDef group : fetchGroups)
			if (groupName.equals(group.getName()))
				return group;
		return null;
	}

	/**
	 * @return ForeignKeyDef[] or <b>null</b> if this TypeDef has no foreign keys.
	 */
	@Override
	public ForeignKeyDef[] getForeignKeys() {
		if (fks.size()>0)
			return fks.toArray(new ForeignKeyDef[fks.size()]);
		else
			return null;
	}
	
	/**
	 * @return IdentityType
	 */
	@Override
	public IdentityType getIdentityType() {
		return identityType;
	}

	/**
	 * @return NonUniqueIndexDef[] or TypeDef.NoIndices if there aren't any non unique indexes.
	 */
	@Override
	public NonUniqueIndexDef[] getIndices() {
		if (nonUniqueIndexes.size()>0)
			return nonUniqueIndexes.toArray(new NonUniqueIndexDef[nonUniqueIndexes.size()]);
		else
			return NoIndices;
	}

	/**
	 * <p>This method is not implemented and always throws JDOUnsupportedOptionException.</p>
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public InheritanceMetadata getInheritanceMetadata() throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("TypeDef does not support getInheritanceMetadata()");
	}

	/**
	 * @return JoinDef[] or TypeDef.NoJoins there aren't any joins.
	 */
	@Override
	public JoinDef[] getJoins() {
		if (getNumberOfJoins()>0)
			return joins.toArray(new JoinDef[joins.size()]);
		else
			return NoJoins;
	}

	/**
	 * <p>Get array of member fields of this TypeDef.</p>
	 * @return FieldDef[]
	 */
	@Override
	public FieldDef[] getMembers() {
		final int count = getNumberOfColumns();
		FieldDef[] members = new FieldDef[count];
		int m = 0;
		if (columns!=null) {
			for (ColumnDef column : columns) {
				FieldDef member = new FieldDef();
				member.setName(column.getName());
				member.setColumn(column.getName());
				member.setFieldType(column.getJDBCType());
				member.setIndexed(column.isIndexed());
				member.setPrimaryKey(column.isPrimaryKey());
				member.setTable(column.getTableName());
				if (column.getIndexType()==null)
					member.setUnique(false);
				else
					member.setUnique(IndexDef.Type.ONE_TO_ONE.equals(column.getIndexType()));
				members[m++] = member;
			}			
		}
		return members;
	}

	/**
	 * <p>Set name of this TypeDef.</p>
	 * @param name String
	 * @return TypeMetadata <b>this</b>
	 */
	public TypeMetadata setName(String name) {
		this.name = name;
		return this;
	}
	
	/**
	 * @return int
	 */
	@Override
	public int getNumberOfFetchGroups() {		
		return fetchGroups==null ? 0 : fetchGroups.size();
	}

	/**
	 * @return int
	 */
	@Override
	public int getNumberOfForeignKeys() {
		return fks==null ? 0 : fks.size();
	}

	/**
	 * @return int
	 */
	@Override
	public int getNumberOfIndices() {
		return nonUniqueIndexes==null ? 0 : nonUniqueIndexes.size();
	}

	/**
	 * @return int
	 */
	@Override
	public int getNumberOfJoins() {
		return joins==null ? 0 : joins.size();
	}

	/**
	 * @return int
	 */
	@Override
	public int getNumberOfMembers() {
		return getNumberOfColumns();
	}

	/**
	 * <p>This method always returns zero.</p>
	 * @return int 0
	 */
	@Override
	public int getNumberOfQueries() {
		return 0;
	}

	/**
	 * @return int
	 */
	@Override
	public int getNumberOfUniques() {
		return uniqueIndexes==null ? 0 : uniqueIndexes.size();
	}

	/**
	 * @return String
	 */
	@Override
	public String getObjectIdClass() {
		return objectIdClass;
	}

	/**
	 * <p>This method always returns <b>null</b>.</p>
	 * @return <b>null</b>
	 */
	@Override
	public QueryMetadata[] getQueries() {
		return null;
	}

	/**
	 * @return boolean
	 */
	@Override
	public boolean getRequiresExtent() {
		return requiresExtent;
	}

	/**
	 * @return String Schema name or empty String or <b>null</b>
	 */
	@Override
	public String getSchema() {
		return schema;
	}

	/**
	 * @return boolean
	 */
	@Override
	public boolean getSerializeRead() {
		return serializeRead;
	}

	/**
	 * @return String Table name
	 */
	@Override
	public String getTable() {
		return table;
	}

	/**
	 * @return UniqueIndexDef[] or TypeDef.NoUniques if there aren't any unique indexes.
	 */
	@Override
	public UniqueIndexDef[] getUniques() {		
		if (getNumberOfUniques()>0)
			return uniqueIndexes.toArray(new UniqueIndexDef[getNumberOfUniques()]);
		else
			return NoUniques;
	}

	/**
	 * <p>This method is not implemented and always throws JDOUnsupportedOptionException.</p>
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public VersionMetadata getVersionMetadata() throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("TypeDef does not support getVersionMetadata()");
	}

	/**
	 * <p>Add a column to this TypeDef.</p>
	 * @param cdef ColumnDef
	 * @throws JDOException if the column position in the given ColumnDef does not match the next available column position at <b>this</b> TypeDef.
	 * @throws JDOUserException if <b>this</b> is unmodifiable or another column with the same name is already present at this TypeDef.
	 * @throws NullPointerException if ColumnDef is <b>null</b>
	 */
	public void addColumnMetadata(ColumnDef cdef) throws JDOException, JDOUserException, NullPointerException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		if (cdef==null)
			throw new NullPointerException("TypeDef.addColumnMetadata(ColumnDef) ColumnDef must not be null");
		if (DebugFile.trace)
			DebugFile.writeln("TypeDef.addColumnMetadata(ColumnDef {"+cdef.getName()+","+String.valueOf(cdef.getPosition())+"}) at position "+String.valueOf(getNumberOfColumns()+1));
		if (positions.containsKey(cdef.getName()))
			throw new JDOUserException("Column "+cdef.getName()+" is already present at "+getName());
		if (cdef.getPosition()!=getNumberOfColumns()+1)
			throw new JDOException("Column "+cdef.getName()+" declared position "+String.valueOf(cdef.getPosition())+" at table "+cdef.getTableName()+" does not match its position at columns list "+String.valueOf(getNumberOfColumns()+1));
		columns.add(cdef);
		positions.put(cdef.getName(), columns.size()-1);
		if (cdef.isPrimaryKey()) {
			if (null==pk)
				pk = new PrimaryKeyDef();
			pk.addColumn(cdef);
		}
	}
	
	/**
	 * <p>Add a column to this TypeDef.</p>
	 * @param columnFamilyName String. Optional.
	 * @param columnName String
	 * @param columnType int one of java.sql.Types
	 * @param maxLength int
	 * @param decimalDigits int
	 * @param isNullable boolean
	 * @param indexType NonUniqueIndexDef.Type
	 * @param foreignKeyTableName String
	 * @param defaultValue String
	 * @param isPrimaryKey boolean <b>true</b> if the column is part of the primary key or <b>false</b> otherwise.
	 * @throws JDOUserException if <b>this</b> is unmodifiable or another column with the same name is already present at this TypeDef.
	 */
	public void addColumnMetadata(String columnFamilyName, String columnName, int columnType, int maxLength, int decimalDigits, boolean isNullable, NonUniqueIndexDef.Type indexType, String foreignKeyTableName, String defaultValue, boolean isPrimaryKey)
			throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		if (positions.containsKey(columnName))
			throw new JDOUserException("Column "+columnName+" is already present at "+getName());
		ColumnDef colDef = newColumnMetadata();		
		if (DebugFile.trace)
			DebugFile.writeln("TypeDef.addColumnMetadata("+colDef.getName()+","+ColumnDef.typeName(columnType)+","+String.valueOf(maxLength)+","+String.valueOf(decimalDigits)+","+indexType+","+foreignKeyTableName+","+defaultValue+","+(isPrimaryKey ? "true" : "false")+","+String.valueOf(colDef.getPosition())+") at position "+String.valueOf(getNumberOfColumns()+1));
		colDef.setFamily(columnFamilyName);
		colDef.setName(columnName);
		colDef.setType(columnType);
		colDef.setSQLType(ColumnDef.typeName(columnType));
		colDef.setJDBCType(ColumnDef.typeName(columnType));
		colDef.setLength(maxLength);
		colDef.setScale(decimalDigits);
		colDef.setAllowsNull(isNullable);
		colDef.setIndexType(indexType);
		colDef.setTarget(foreignKeyTableName);
		colDef.setDefaultValue(defaultValue);
		colDef.setPrimaryKey(isPrimaryKey);
		positions.put(columnName, columns.size()-1);
		if (colDef.isPrimaryKey()) {
			if (null==pk)
				pk = new PrimaryKeyDef();
			pk.addColumn(colDef);
		}
	}
	
	/**
	 * <p>Add a column to this TypeDef.</p>
	 * @param columnFamilyName String. Optional.
	 * @param columnName String
	 * @param columnType int one of java.sql.Types
	 * @param maxLength int
	 * @param decimalDigits int
	 * @param isNullable boolean
	 * @param indexType NonUniqueIndexDef.Type
	 * @param foreignKeyTableName String
	 * @param defaultValue String
	 * @throws JDOUserException if <b>this</b> is unmodifiable or another column with the same name is already present at this TypeDef.
	 */
	public void addColumnMetadata(String columnFamilyName, String columnName, int columnType, int maxLength, int decimalDigits, boolean isNullable, NonUniqueIndexDef.Type indexType, String foreignKeyTableName, String defaultValue) {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		if (positions.containsKey(columnName))
			throw new JDOUserException("Column "+columnName+" is already present at "+getName());
		ColumnDef colDef = newColumnMetadata();
		if (DebugFile.trace)
			DebugFile.writeln("TypeDef.addColumnMetadata("+colDef.getName()+","+ColumnDef.typeName(columnType)+","+String.valueOf(maxLength)+","+String.valueOf(decimalDigits)+","+indexType+","+foreignKeyTableName+","+defaultValue+","+String.valueOf(colDef.getPosition())+") no primary key at position "+String.valueOf(getNumberOfColumns()+1));
		colDef.setFamily(columnFamilyName);
		colDef.setName(columnName);
		colDef.setType(columnType);
		colDef.setJDBCType(ColumnDef.typeName(columnType));
		colDef.setLength(maxLength);
		colDef.setScale(decimalDigits);
		colDef.setAllowsNull(isNullable);
		colDef.setIndexType(indexType);
		colDef.setTarget(foreignKeyTableName);
		colDef.setDefaultValue(defaultValue);
		colDef.setPrimaryKey(false);
		positions.put(columnName, columns.size()-1);
	}

	protected void clearColumnsMeta() {
		columns.clear();
		positions.clear();
		if (null!=pk)
			pk.clear();
	}
	
	/**
	 * <p>This method is not implemented and always throws JDOUnsupportedOptionException.</p>
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public DatastoreIdentityMetadata newDatastoreIdentityMetadata() throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("TypeDef does not support newDatastoreIdentityMetadata()");
	}

	/**
	 * <p>Add a new MemberGroupDef with the given name.</p>
	 * @param groupName String
	 * @return MemberGroupDef
	 */
	@Override
	public MemberGroupDef newFetchGroupMetadata(String groupName) {
		MemberGroupDef group = new MemberGroupDef(groupName);
		fetchGroups.add(group);
		return group;
	}

	/**
	 * <p>Add a foreign key to this TypeDef.</p>
	 * @param fk ForeignKeyDef
	 * @throws JDOUserException if <b>this</b> is unmodifiable or another foreign key with the same name is already present at this TypeDef.
	 */
	public void addForeignKeyMetadata(ForeignKeyDef fk) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		if (fk.getName()!=null && fk.getName().length()>0)
			for (ForeignKeyMetadata fkm : fks)
				if (fk.getName().equalsIgnoreCase(fkm.getName()))
					throw new JDOUserException("Foreign key "+fk.getName()+" already exists at "+getName());
		fks.add(fk);
	}

	/**
	 * <p>Add a non unique index to this TypeDef.</p>
	 * @param index NonUniqueIndexDef
	 * @throws JDOUserException if <b>this</b> is unmodifiable or another index with the same name is already present at this TypeDef.
	 */
	public void addIndexMetadata(NonUniqueIndexDef index) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		final String indexName = index.getName();
		if (indexName!=null && indexName.length()>0) {
			for (IndexMetadata idx : getIndices())
				if (indexName.equalsIgnoreCase(idx.getName()))
					throw new JDOUserException("Index "+indexName+" already exists at "+getName());
		}
		nonUniqueIndexes.add(index);
	}
	
	/**
	 * <p>This method is not implemented and always throws JDOUnsupportedOptionException.</p>
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public InheritanceMetadata newInheritanceMetadata() throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("TypeDef doe snot support newInheritanceMetadata()");
	}

	/**
	 * <p>Create a new PrimaryKeyDef.</p>
	 * The newly created PrimaryKeyDef is not automatically added to primary of this TypeDef but must be added manually by calling setPrimaryKeyMetadata()
	 * @return PrimaryKeyDef
	 * @throws JDOUserException if <b>this</b> is unmodifiable.
	 */
	@Override
	public PrimaryKeyDef newPrimaryKeyMetadata() throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		pk = new PrimaryKeyDef();
		return pk;
	}

	/**
	 * <p>Set primary key of this TypeDef.</p>
	 * @param pk PrimaryKeyDef
	 * @throws JDOUserException if <b>this</b> is unmodifiable.
	 */
	public void setPrimaryKeyMetadata(PrimaryKeyDef pk) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.pk = pk;
		if (pk!=null)
			for (ColumnMetadata cdef : pk.getColumns())
				getColumnByName(cdef.getName()).setPrimaryKey(true);
	}
	
	/**
	 * <p>This method is not implemented and always throws JDOUnsupportedOptionException.</p>
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public PropertyMetadata newPropertyMetadata(String arg0) throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("TypeDef does not support newPropertyMetadata()");
	}

	/**
	 * <p>This method is not implemented and always throws JDOUnsupportedOptionException.</p>
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public PropertyMetadata newPropertyMetadata(Method arg0) throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("TypeDef does not support newPropertyMetadata()");
	}

	/**
	 * <p>This method is not implemented and always throws JDOUnsupportedOptionException.</p>
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public QueryMetadata newQueryMetadata(String queryName) throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("TypeDef does not support newQueryMetadata()");
	}

	/**
	 * <p>Add unique index.</p>
	 * @param index UniqueIndexDef
	 * @throws JDOUserException if <b>this</b> is unmodifiable or another with the same name already exists at this TypeDef.
	 */
	public void addUniqueMetadata(UniqueIndexDef index) throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		final String indexName = index.getName();
		if (indexName!=null && indexName.length()>0) {
			for (UniqueMetadata idx : getUniques())
				if (indexName.equalsIgnoreCase(idx.getName()))
					throw new JDOUserException("Index "+indexName+" already exists at "+getName());
		}
		uniqueIndexes.add(index);
	}

	/**
	 * <p>This method is not implemented and always throws JDOUnsupportedOptionException.</p>
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public VersionMetadata newVersionMetadata() throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("TypeDef doe snot support newVersionMetadata()");
	}

	/**
	 * @param cacheable boolean
	 * @return TypeDef <b>this</b>
	 * @throws JDOUserException if <b>this</b> is unmodifiable.
	 */
	@Override
	public TypeDef setCacheable(boolean cacheable) {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		super.cacheable = cacheable;
		return this;
	}

	/**
	 * @param catalog String Catalog Name
	 * @return TypeDef <b>this</b>
	 * @throws JDOUserException if <b>this</b> is unmodifiable.
	 */
	@Override
	public TypeDef setCatalog(String catalog) {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.catalog = catalog;
		return this;
	}

	/**
	 * @param detachable boolean
	 * @return TypeDef <b>this</b>
	 * @throws JDOUserException if <b>this</b> is unmodifiable.
	 */
	@Override
	public TypeDef setDetachable(boolean detachable) {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.detachable = detachable;
		return this;
	}

	/**
	 * @param embeddedOnly boolean
	 * @return TypeDef <b>this</b>
	 * @throws JDOUserException if <b>this</b> is unmodifiable.
	 */
	@Override
	public TypeMetadata setEmbeddedOnly(boolean embeddedOnly) {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.embeddedOnly = embeddedOnly;
		return this;
	}

	/**
	 * @param identityType IdentityType
	 * @return TypeDef <b>this</b>
	 * @throws JDOUserException if <b>this</b> is unmodifiable.
	 */
	@Override
	public TypeMetadata setIdentityType(IdentityType identityType) {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.identityType = identityType;
		return this;
	}

	/**
	 * @param objectClass String
	 * @return TypeDef <b>this</b>
	 * @throws JDOUserException if <b>this</b> is unmodifiable.
	 */
	@Override
	public TypeDef setObjectIdClass(String objectClass) {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.objectIdClass = objectClass;
		return this;
	}

	/**
	 * @param requiresExtent boolean
	 * @return TypeDef <b>this</b>
	 * @throws JDOUserException if <b>this</b> is unmodifiable.
	 */
	@Override
	public TypeDef setRequiresExtent(boolean requiresExtent) {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.requiresExtent = requiresExtent;
		return this;
	}

	/**
	 * @param schema String Schema Name
	 * @return TypeDef <b>this</b>
	 * @throws JDOUserException if <b>this</b> is unmodifiable.
	 */
	@Override
	public TypeDef setSchema(String schema) {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.schema = schema;
		return this;
	}

	/**
	 * @param serializeRead boolean
	 * @return TypeDef <b>this</b>
	 * @throws JDOUserException if <b>this</b> is unmodifiable.
	 */
	@Override
	public TypeDef setSerializeRead(boolean serializeRead) {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.serializeRead = serializeRead;
		return this;
	}

	/**
	 * @param table String Table Name
	 * @return TypeDef <b>this</b>
	 * @throws JDOUserException if <b>this</b> is unmodifiable.
	 */
	@Override
	public TypeMetadata setTable(String table) {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		this.table = table;
		return this;
	}
	
}
