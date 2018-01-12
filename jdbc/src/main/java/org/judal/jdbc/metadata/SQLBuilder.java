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

import org.judal.jdbc.RDBMS;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.TypeDef;

import com.knowgate.debug.DebugFile;

/**
 * <p>Helper functions for loading, storing and deleting rows from a SQL table.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class SQLBuilder {

	private SQLStatements sqlStatements;
	private TypeDef tdef;
	
	// ----------------------------------------------------------

	/**
	 * @param dbms int Integer code from RDMBS enum
	 * @param tdef TypeDef
	 * @param timestampColumn String Name of creation timestamp column. If provided, the value of this column will not be updated after it has been initially set.
	 * @throws SQLException new SQLHelper
	 */
	public SQLBuilder(int dbms, TypeDef tdef, String timestampColumn) throws SQLException {
		this.tdef = tdef;
		sqlStatements = new SQLStatements();
		sqlStatements.setTimestampColumn(timestampColumn);
		precomputeSqlStatements(dbms);
	}

	// ----------------------------------------------------------

	/**
	 * @param dbms RDBMS
	 * @param tdef TypeDef
	 * @param timestampColumn String Name of creation timestamp column. If provided, the value of this column will not be updated after it has been initially set.
	 * @throws SQLException
	 */
	public SQLBuilder(RDBMS dbms, TypeDef tdef, String timestampColumn) throws SQLException {
		this.tdef = tdef;
		sqlStatements = new SQLStatements();
		sqlStatements.setTimestampColumn(timestampColumn);
		precomputeSqlStatements(dbms.intValue());
	}
	
	// ----------------------------------------------------------

	public SQLStatements getSqlStatements() {
		return sqlStatements;
	}
	
	// ----------------------------------------------------------

	private void precomputeSqlStatements(int dbms) throws SQLException {

		String insertAllCols = "";
		String getAllCols = "";
		String setAllCols = "";
		String setPkCols = "";
		String setNoPkCols = "";

		if (DebugFile.trace)
		{
			DebugFile.writeln("Begin SQLBuilder.precomputeSqlStatements(" + RDBMS.valueOf(dbms)+ ")" );
			DebugFile.incIdent();
			DebugFile.writeln("catalog is " + tdef.getCatalog());
			DebugFile.writeln("schema is " + tdef.getSchema());
			DebugFile.writeln("table or view is " + tdef.getName());
		}

		for (ColumnDef column : tdef.getColumns()) {
			String columnName = column.getName();
			if (DebugFile.trace)
				DebugFile.writeln("reading column "+columnName+" of type "+((SQLColumn) column).getSqlTypeName());

			if (column.isPrimaryKey())
				setPkCols += column.getName() + "=? AND ";

			if (dbms==RDBMS.POSTGRESQL.intValue()) {
				if (((SQLColumn) column).getSqlTypeName().equalsIgnoreCase("geography")) {
					insertAllCols  += columnName+",";
					getAllCols  += "ST_X("+columnName+"::geometry)||' '||ST_Y("+columnName+"::geometry) AS "+columnName+",";        	  
					setAllCols  += "ST_SetSRID(ST_MakePoint(?,?),4326),";
					setNoPkCols += columnName + "=ST_SetSRID(ST_MakePoint(?,?),4326),";
				} else if (((SQLColumn) column).getSqlTypeName().equalsIgnoreCase("serial")) {
					getAllCols += columnName + ",";
				} else {
					insertAllCols += columnName + ",";        	 
					getAllCols += columnName + ",";
					setAllCols += "?,";
					if (!column.isPrimaryKey() && !columnName.equalsIgnoreCase(sqlStatements.getTimestampColumn()))
						setNoPkCols += columnName + "=?,";
				}  
			} else {
				if (column.getAutoIncrement()) {
					getAllCols += columnName + ",";        		
				} else {
					insertAllCols += columnName + ",";        	 
					getAllCols += columnName + ",";
					setAllCols += "?,";
					if (!column.isPrimaryKey() && !columnName.equalsIgnoreCase(sqlStatements.getTimestampColumn()))
						setNoPkCols += columnName + "=?,";        		
				}
			}
		} // wend

		if (setPkCols.length()>0)
			setPkCols = setPkCols.substring(0, setPkCols.length()-5);

		if (DebugFile.trace) DebugFile.writeln("get all cols " + getAllCols );

		if (getAllCols.length()>0)
			getAllCols = getAllCols.substring(0, getAllCols.length()-1);
		else
			getAllCols = "*";

		if (insertAllCols.length()>0)
			insertAllCols = insertAllCols.substring(0, insertAllCols.length()-1);

		if (DebugFile.trace) DebugFile.writeln("set all cols " + setAllCols );

		if (setAllCols.length()>0)
			setAllCols = setAllCols.substring(0, setAllCols.length()-1);

		if (DebugFile.trace) DebugFile.writeln("set no pk cols " + setNoPkCols );

		if (setNoPkCols.length()>0)
			setNoPkCols = setNoPkCols.substring(0, setNoPkCols.length()-1);

		if (DebugFile.trace) DebugFile.writeln("set pk cols " + setPkCols );

		if (setPkCols.length()>0) {
			sqlStatements.setSelect("SELECT " + getAllCols + " FROM " + tdef.getName() + " WHERE " + setPkCols);
			sqlStatements.setInsert("INSERT INTO " + tdef.getName() + "(" + insertAllCols + ") VALUES (" + setAllCols + ")");
			if (setNoPkCols.length()>0)
				sqlStatements.setUpdate("UPDATE " + tdef.getName() + " SET " + setNoPkCols + " WHERE " + setPkCols);
			else
				sqlStatements.setUpdate(null);
			sqlStatements.setDelete("DELETE FROM " + tdef.getName() + " WHERE " + setPkCols);
			sqlStatements.setExists("SELECT NULL FROM " + tdef.getName() + " WHERE " + setPkCols);
			if (DebugFile.trace) {
				DebugFile.writeln("Generated SQL statements for "+tdef.getName());
				DebugFile.writeln("Generated "+sqlStatements.getSelect());
				DebugFile.writeln("Generated "+sqlStatements.getInsert());
				DebugFile.writeln("Generated "+sqlStatements.getUpdate());
				DebugFile.writeln("Generated "+sqlStatements.getDelete());
			}
		} 
		else {
			sqlStatements.setSelect(null);
			sqlStatements.setInsert("INSERT INTO " + tdef.getName() + "(" + insertAllCols + ") VALUES (" + setAllCols + ")");
			sqlStatements.setUpdate(null);
			sqlStatements.setDelete(null);
			sqlStatements.setExists(null);
			if (DebugFile.trace) {
				DebugFile.writeln("WARNING No Primary Key found for table " + tdef.getName());
				DebugFile.writeln("Generated SQL statement for "+tdef.getName());
				DebugFile.writeln("Generated "+sqlStatements.getInsert());
			}
		}


		if (DebugFile.trace)
		{
			DebugFile.decIdent();
			DebugFile.writeln("End SQLBuilder.precomputeSqlStatements()");
		}

	} // precomputeSqlStatements
	
}