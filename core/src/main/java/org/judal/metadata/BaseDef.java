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
import java.util.LinkedList;
import java.util.List;

import javax.jdo.JDOUserException;

import com.knowgate.debug.DebugFile;

/**
 * <p>Base class for several types of metadata objects.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public abstract class BaseDef extends ExtendableDef implements Cloneable {

	protected String name;
	protected String table;
	protected ArrayList<ColumnDef> columns;
	protected List<JoinDef> joins;
	protected PrimaryKeyDef pk;
	protected List<ForeignKeyDef> fks;
	protected boolean cacheable;
	protected boolean embeddedOnly;
	protected List<UniqueIndexDef> uniqueIndexes;
	protected List<NonUniqueIndexDef> nonUniqueIndexes;
	private boolean unmodifiable;

	protected static final JoinDef[] NoJoins = new JoinDef[0];
	protected static final ColumnDef[] NoColumns = new ColumnDef[0];
	protected static final UniqueIndexDef[] NoUniques = new UniqueIndexDef[0];
	protected static final NonUniqueIndexDef[] NoIndices = new NonUniqueIndexDef[0];

	/**
	 * Default Constructor
	 */
	public BaseDef() {
		columns = new ArrayList<ColumnDef>();
		joins = new LinkedList<JoinDef>();
		pk = null;
		fks = new LinkedList<ForeignKeyDef>();
		uniqueIndexes = new LinkedList<UniqueIndexDef>();
		nonUniqueIndexes = new LinkedList<NonUniqueIndexDef>();
		cacheable = true;
		unmodifiable = false;
	}

	/**
	 * Create a BaseDef by cloning an already existing one
	 * @param source BaseDef
	 */
	public BaseDef(BaseDef source) {
		this.name = source.name;
		this.table = source.table;
		this.columns = new ArrayList<ColumnDef>(source.columns);
		this.joins = new LinkedList<JoinDef>(source.joins);
		this.pk = source.pk;
		this.fks = new LinkedList<ForeignKeyDef>(source.fks);
		this.uniqueIndexes = new LinkedList<UniqueIndexDef>(source.uniqueIndexes);
		this.nonUniqueIndexes = new LinkedList<NonUniqueIndexDef>(source.nonUniqueIndexes);
		this.cacheable = source.cacheable;
		this.unmodifiable = false;
	}

	@Override
	/**
	 * <p>Required by Cloneable interface.</p>
	 * @return BaseDef
	 */
	public abstract BaseDef clone();

	public String getName() {
		return name;
	}

	public boolean getCacheable() {
		return cacheable;
	}

	public int getNumberOfColumns() {
		return columns.size();
	}

	/**
	 * <p>Get copy of array of ColumnDef</p>
	 * Changes made on the returned array will not affect this object.
	 * @return ColumnDef[]
	 */
	public ColumnDef[] getColumns() {
		if (getNumberOfColumns()>0)
			return columns.toArray(new ColumnDef[getNumberOfColumns()]);
		else
			return NoColumns;
	}

	/**
	 * <p>Filter columns with given names.</p>
	 * Name matching is case insensitive.
	 * @param columnNames String[] Names of columns to be returned
	 * @return Array of columns which names matches one of the given list
	 * @throws ArrayIndexOutOfBoundsException if any column name does not match the name of any existing column
	 */
	public ColumnDef[] filterColumns(String[] columnNames) throws ArrayIndexOutOfBoundsException {
		final int colCount = columnNames.length;
		ColumnDef[] filteredCols = new ColumnDef[colCount];
		for (int c=0; c<colCount; c++) {
			boolean found = false;
			final String colName = columnNames[c];
			for (ColumnDef cdef : columns) {
				if (cdef.getName().equalsIgnoreCase(colName)) {
					filteredCols[c++] = cdef;
					found = true;
					break;
				}
			}
			if (!found)
				throw new ArrayIndexOutOfBoundsException("Column "+colName+" bot found at "+getName());
		}
		return filteredCols;
	}
	
	public PrimaryKeyDef getPrimaryKeyMetadata() {
		return pk;
	}

	public String getTable() {
		return table;
	}

	/**
	 * <p>After setUnmofiable() is called, no calls to newColumnMetadata() newJoinMetadata() newForeignKeyMetadata() newIndexMetadata() or newUniqueMetadata() will be disallowed by throwing a JDOUserException.</p>
	 */
	public void setUnmodifiable() {
		unmodifiable = true;
	}

	/** Create a new column without adding it to the table definition.
	 * Override this method at BaseDef subclasses to get specialized types of column definitions
	 * return ColumnDef
	 */
	protected ColumnDef createColumn() {
		return new ColumnDef();
	}
	
	/**
	 * <p>Create a new column and add it to this object.</p>
	 * @return ColumnDef Newly created column
	 * @throws JDOUserException If this instance has been set as unmodifiable
	 */
	public ColumnDef newColumnMetadata() throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		ColumnDef cdef = createColumn();
		if (DebugFile.trace) DebugFile.writeln("newColumnMetadata() at position "+String.valueOf(getNumberOfColumns()+1));
		cdef.setPosition(getNumberOfColumns()+1);
		columns.add(cdef);
		return cdef;
	}
	
	/**
	 * Create a new join metadata and add it to this object
	 * @return JoinDef Newly created join metadata
	 * @throws JDOUserException If this instance has been set as unmodifiable
	 */
	public JoinDef newJoinMetadata() throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		JoinDef join = new JoinDef();
		joins.add(join);
		return join;
	}
	
	/**
	 * Create a new foreign key metadata and add it to this object
	 * @return ForeignKeyDef Newly created foreign key metadata
	 * @throws JDOUserException If this instance has been set as unmodifiable
	 */
	public ForeignKeyDef newForeignKeyMetadata() throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		ForeignKeyDef fk = new ForeignKeyDef();
		fks.add(fk);
		return fk;
	}
	
	/**
	 * Create a new index metadata and add it to this object
	 * @return IndexDef Newly created index metadata
	 * @throws JDOUserException If this instance has been set as unmodifiable
	 */
	public NonUniqueIndexDef newIndexMetadata() throws JDOUserException {
		if (getUnmodifiable())
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		NonUniqueIndexDef idx = new NonUniqueIndexDef();
		nonUniqueIndexes.add(idx);
		return idx;
	}
	
	/**
	 * Create a new unique metadata and add it to this object
	 * @return UniqueIndexDef Newly created unique metadata
	 * @throws JDOUserException If this instance has been set as unmodifiable
	 */
	public UniqueIndexDef newUniqueMetadata() throws JDOUserException {
		if (unmodifiable)
			throw new JDOUserException(getClass().getName().substring(getClass().getName().lastIndexOf('.')+1)+" is set to unmodifiable");
		UniqueIndexDef unique = new UniqueIndexDef();
		uniqueIndexes.add(unique);
		return unique;
	}

	protected boolean getUnmodifiable() {
		return unmodifiable;
	}
	
}
