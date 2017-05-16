package org.judal.jdbc.metadata;

import java.sql.Types;

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

import javax.jdo.metadata.ColumnMetadata;

import org.judal.jdbc.RDBMS;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.NonUniqueIndexDef;

public class SQLIndex extends NonUniqueIndexDef {

	private static final long serialVersionUID = 1L;

	public SQLIndex(String sTableName, String sIndexName, String sColumnName, boolean bIsUnique) {
		super(sTableName, sIndexName, new ColumnDef(sColumnName, Types.NULL, 1),
				bIsUnique ? Type.ONE_TO_ONE : Type.MANY_TO_ONE);
	}

	public SQLIndex(String sTableName, String sIndexName, String[] aIndexColumns, boolean bIsUnique) {
		super(sTableName, sIndexName, map(aIndexColumns), bIsUnique ? Type.ONE_TO_ONE : Type.MANY_TO_ONE);
	}

	public SQLIndex(String sTableName, String sIndexName, String[] aIndexColumns, boolean bIsUnique, Using eUsing) {
		super(sTableName, sIndexName, map(aIndexColumns), bIsUnique ? Type.ONE_TO_ONE : Type.MANY_TO_ONE, eUsing);
	}

	private static ColumnDef[] map(String[] aIndexColumns) {
		ColumnDef[] cols = new ColumnDef[aIndexColumns.length];
		for (int c = 0; c < aIndexColumns.length; c++)
			cols[c] = new ColumnDef(aIndexColumns[c], Types.NULL, c + 1);
		return cols;
	}

	/*
	 * public JDCIndex (String sTableName, String sIndexName, List<String>
	 * oIndexColumns, boolean bIsUnique) { super(sTableName, sIndexName,
	 * oIndexColumns, bIsUnique ? Type.ONE_TO_ONE : Type.MANY_TO_ONE); }
	 * 
	 */

	public String sqlScriptDef(RDBMS eRDBMS) {
		StringBuffer sql = new StringBuffer();
		sql.append("CREATE ");
		if (Type.ONE_TO_ONE.equals(getType()))
			sql.append("UNIQUE ");
		sql.append("INDEX ");
		sql.append(getName());
		sql.append(" ON ");
		sql.append(getTable());
		if (getUsing() != null)
			sql.append(" USING " + getUsing() + " ");
		sql.append(" (");
		String[] colNames = new String[getNumberOfColumns()];
		int c = 0;
		for (ColumnMetadata col : getColumns())
			colNames[c++] = col.getName();
		sql.append(String.join(",", colNames));
		sql.append(")");
		return sql.toString();
	}

}
