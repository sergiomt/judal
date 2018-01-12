package org.judal.jdbc.metadata;

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

import java.sql.SQLException;
import java.util.ArrayList;

import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.JDOUserException;
import javax.jdo.metadata.ForeignKeyMetadata;
import javax.jdo.metadata.JoinMetadata;

import org.judal.jdbc.RDBMS;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.NameAlias;
import org.judal.metadata.ViewDef;
import org.judal.storage.Param;

import com.knowgate.debug.DebugFile;

public class SQLViewDef extends ViewDef implements SQLSelectableDef {

	private static final long serialVersionUID = 1L;
	
	private RDBMS dbms;
	private String aliasedName;
	private String viewSource;
	
	public SQLViewDef(RDBMS dbms, String aliasedName, String viewSource) throws JDOUserException, SQLException {
		super(aliasedName);
		final NameAlias nameAlias = NameAlias.parse(aliasedName);
		this.dbms = dbms;
		this.viewSource = viewSource;
		this.aliasedName = aliasedName;
		setName(nameAlias.getName());
		setAlias(nameAlias.getAlias());
		setCatalog(null);
		setSchema(null);
		if (DebugFile.trace)
			DebugFile.writeln("new SQLViewDef(" + dbms + "," + aliasedName + "," + viewSource + ")");
	}

	/**
	 * @return SQLViewDef
	 */
	@Override
	public SQLViewDef clone() {
		SQLViewDef theClone = null;
		try {
			theClone = new SQLViewDef(dbms, aliasedName, viewSource);
			theClone.setCatalog(getCatalog());
			theClone.setSchema(getSchema());
			for (ColumnDef cdef : getColumns())
				if (cdef instanceof SQLColumn )
					theClone.addColumnMetadata(new SQLColumn((SQLColumn) cdef));
				else
					theClone.addColumnMetadata(new ColumnDef(cdef));
			theClone.autoSetPrimaryKey();
		} catch (SQLException sqle) { }
		return theClone;
	}
	
	/**
	 * <p>Assign a reference to a column list.</p>
	 * Changes made in coldefs will be reflected in this view.
	 * @param coldefs ArrayList&lt;ColumnDef&gt;
	 */
	@Override
	public void setColumns(ArrayList<ColumnDef> coldefs) {
		if (null==coldefs)
			columns = new ArrayList<ColumnDef>(0);
		else
			columns = coldefs;
		autoSetPrimaryKey();
	}
	
	public String getSource() {
		return viewSource;
	}

	public String getDrop() {
		return "DROP VIEW "+getName();
	}

	@Override
	public Param[] getParams() {
		return new Param[0];
	}

	// ---------------------------------------------------------------------------

	/**
	 * Get table names including the joined tables if present
	 * @return String of the form "this_table_name [INNER|OUTER] JOIN joined_table_name ON this_column_name=joined_column_name"
	 */
	@Override
	public String getTables() throws JDOUserException,JDOUnsupportedOptionException {

		String retval = null;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin SQLViewDef.getTables()");
			DebugFile.incIdent();
			DebugFile.writeln("Name is " + getName());
			DebugFile.writeln("Alias is " + getAlias());
			DebugFile.writeln("Number of joins is " + getNumberOfJoins());
		}
			
		if (getNumberOfJoins()==0) {
			if (getAlias()==null || getAlias().length()==0 || getName().equals(getAlias()))
				retval = getName();
			else
				retval = getName() + " " + getAlias();
		} else if (getNumberOfJoins()==1) {
			JoinMetadata join = getJoins()[0];
			for (ForeignKeyMetadata fk : getForeignKeys()) {
				if (fk.getTable().equals(join.getTable())) {
					if (fk.getNumberOfColumns()!=join.getNumberOfColumns())
						throw new JDOUserException("Foreign key "+(fk.getName()==null ? "" : fk.getName()) +" number of columns "+fk.getNumberOfColumns()+" does not match join number of columns "+join.getNumberOfColumns());
					StringBuilder joinSql = new StringBuilder();
					NameAlias joinedTable = NameAlias.parse(join.getTable());
					joinSql.append(getName()).append(" AS ").append(getAlias());
					joinSql.append(" ").append(join.getOuter() ? "OUTER" : "INNER").append(" JOIN ");
					joinSql.append(join.getTable()).append(" ON ");
					joinSql.append(getAlias());
					joinSql.append(".").append(fk.getColumns()[0].getName());
					joinSql.append("=");
					joinSql.append(joinedTable.getAlias()!=null ? joinedTable.getAlias() : joinedTable.getName());
					joinSql.append(".").append(join.getColumn());
					if (fk.getNumberOfColumns()>1) {
						for (int c=1; c<fk.getNumberOfColumns(); c++) {
							joinSql.append(" AND ");
							joinSql.append(getAlias()).append(".").append(fk.getColumns()[c].getName());
							joinSql.append("=");
							joinSql.append(joinedTable.getAlias()!=null ? joinedTable.getAlias() : joinedTable.getName());
							joinSql.append(".").append(join.getColumns()[c]);
						}							
					}
					retval = joinSql.toString();
				}
			}
			if (null==retval) {
				if (DebugFile.trace) DebugFile.decIdent();
				throw new JDOUserException("Cannot find a foreign key corresponding to join definition");
			}
		} else {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new JDOUnsupportedOptionException("Only one join at a time is supported");
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End SQLViewDef.getTables() : " + retval);
		}
		
		return retval;
	}
	
}
