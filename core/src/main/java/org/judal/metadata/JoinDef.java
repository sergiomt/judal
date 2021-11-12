package org.judal.metadata;

/*
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

import java.util.LinkedList;

import javax.jdo.annotations.ForeignKeyAction;
import javax.jdo.metadata.Indexed;
import javax.jdo.metadata.JoinMetadata;
import javax.jdo.metadata.UniqueMetadata;

/**
 * JDO JoinMetadata interface implementation
 * @author Sergio Montoro Ten
 *
 */
public class JoinDef extends ExtendableDef implements JoinMetadata {

	private static final long serialVersionUID = 10000l;

	private String table;
	private Indexed indexed;
	private JoinType joinType;
	private LinkedList<ColumnDef> cols;
	private UniqueIndexDef index;
	private ForeignKeyDef fk;
	private PrimaryKeyDef pk;
	
	public JoinDef() {
		pk = null;
		fk = null;
		table = null;
		joinType = JoinType.INNER;
		index = null;
		indexed = Indexed.UNSPECIFIED;
		cols = new LinkedList<ColumnDef>();
	}

	/**
	 * Name of the joined column
	 * @return String
	 */
	@Override
	public String getColumn() {
		if (cols.size()>=0)
			return cols.getFirst().getName();
		else
			return null;
	}

	/**
	 * List of columns from the joined table
	 * @return ColumnDef[]
	 */
	@Override
	public ColumnDef[] getColumns() {
		return cols.toArray(new ColumnDef[cols.size()]);
	}

	@Override
	public ForeignKeyAction getDeleteAction() {
		return fk.getDeleteAction();
	}

	/**
	 * Foreign key data on the base table
	 * @return ForeignKeyDef
	 */
	@Override
	public ForeignKeyDef getForeignKeyMetadata() {
		return fk;
	}

	/**
	 * @return IndexDef
	 */
	@Override
	public IndexDef getIndexMetadata() {
		return index;
	}

	/**
	 * @return Indexed
	 */
	@Override
	public Indexed getIndexed() {
		return indexed;
	}

	/**
	 * Number of columns used to match rows
	 * @return int
	 */
	@Override
	public int getNumberOfColumns() {
		return cols.size();
	}

	/**
	 * @return <b>true</b> if this is an outer join, <b>false</b> it if is an inner join
	 */
	@Override
	public boolean getOuter() {
		return JoinType.OUTER.equals(joinType);
	}

	/**
	 * @return PrimaryKeyDef
	 */
	@Override
	public PrimaryKeyDef getPrimaryKeyMetadata() {
		return pk;
	}

	/**
	 * Name of the joined table
	 * @return String
	 */
	@Override
	public String getTable() {
		return table;
	}

	/**
	 * Get whether getIndexed() will return a unique index
	 * @return Boolean
	 */
	@Override
	public Boolean getUnique() {
		return indexed.equals(Indexed.UNIQUE);
	}

	@Override
	public UniqueMetadata getUniqueMetadata() {
		return index;	
	}

	/**
	 * Create a new ColumnDef instance and add it to the list of columns of the joined table
	 * @return ColumnDef
	 */
	@Override
	public ColumnDef newColumnMetadata() {
		ColumnDef cdef = new ColumnDef();
		cols.add(cdef);
		return cdef;
	}

	/**
	 * Create new instance of ForeignKeyDef and assign it to this JoinDef
	 * @return ForeignKeyDef
	 */
	@Override
	public ForeignKeyDef newForeignKeyMetadata() {
		fk = new ForeignKeyDef();
		return fk;
	}

	/**
	 * Create new instance of IndexDef and assign it to this JoinDef
	 * @return IndexDef
	 */
	@Override
	public IndexDef newIndexMetadata() {
		index = new UniqueIndexDef();
		return index;
	}

	/**
	 * Create new instance of PrimaryKeyDef and assign it to this JoinDef
	 * @return PrimaryKeyDef
	 */
	@Override
	public PrimaryKeyDef newPrimaryKeyMetadata() {
		pk = new PrimaryKeyDef();
		return pk;
	}

	/**
	 * Create new instance of UniqueIndexDef and assign it to this JoinDef
	 * @return UniqueIndexDef
	 */
	@Override
	public UniqueIndexDef newUniqueMetadata() {
		index = new UniqueIndexDef();
		index.setUnique(true);
		return index;
	}

	/**
	 * Set name of column used to join the foreign table
	 * @param columnName String Name of the column in the foreign table
	 * @return JoinDef <b>this</b> object
	 */
	@Override
	public JoinDef setColumn(String columnName) {
		cols.clear();
		return addColumn(columnName);
	}

	/**
	 * Add a new column to the list of columns used to join the foreign table
	 * @param columnName String Name of the column in the foreign table
	 * @return JoinDef <b>this</b> object
	 */
	public JoinDef addColumn(String columnName) {
		ColumnDef cdef = newColumnMetadata();
		cdef.setName(columnName);
		return this;
	}
	
	/**
	 * @param action ForeignKeyAction
	 * @return JoinDef <b>this</b> object
	 */
	@Override
	public JoinDef setDeleteAction(ForeignKeyAction action) {
		fk.setDeleteAction(action);
		return this;
	}

	/**
	 * @param indexed Indexed
	 * @return JoinDef <b>this</b> object
	 */
	@Override
	public JoinDef setIndexed(Indexed indexed) {
		this.indexed = indexed;
		return this;
	}

	/**
	 * @param outer boolean
	 * @return JoinDef <b>this</b> object
	 */
	@Override
	public JoinDef setOuter(boolean outer) {
		if (outer)
			this.joinType = JoinType.OUTER;
		else
			this.joinType = JoinType.INNER;
		return this;
	}

	/**
	 * @param table String Foreign key table name
	 * @return JoinDef <b>this</b> object
	 */
	@Override
	public JoinDef setTable(String table) {
		this.table = table;
		return this;
	}

	/**
	 * @param unique boolean
	 * @return JoinDef <b>this</b> object
	 */
	@Override
	public JoinDef setUnique(boolean unique) {
		if (unique)
			indexed = Indexed.UNIQUE;
		else if (indexed.equals(Indexed.UNIQUE))
			indexed = Indexed.TRUE;
		return this;
	}

}
