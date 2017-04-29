package org.judal.jdbc;

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

import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.judal.jdbc.metadata.SQLIndex;
import org.judal.metadata.IndexDef.Using;
import org.judal.storage.Param;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Connective;
import org.judal.storage.query.Operator;
import org.judal.storage.query.Predicate;
import org.judal.storage.query.sql.SQLQuery;
import org.judal.storage.relational.RelationalTable;
import org.judal.storage.table.Record;

import javax.jdo.JDOException;
import javax.jdo.metadata.PrimaryKeyMetadata;

import com.knowgate.debug.DebugFile;
import com.knowgate.gis.LatLong;

public class JDBCRelationalTable extends JDBCRelationalView implements RelationalTable {

	public JDBCRelationalTable(JDBCTableDataSource dataSource, Record recordInstance) throws JDOException {
		super(dataSource, recordInstance);
	}

	@Override
	public PrimaryKeyMetadata getPrimaryKey() {
		return tableDef.getPrimaryKeyMetadata();
	}
	
	@Override
	public String getTimestampColumnName() {
		return getTableDef().getCreationTimestampColumnName();
	}

	@Override
	public void setTimestampColumnName(String columnName) throws IllegalArgumentException {
		getTableDef().setCreationTimestampColumnName(columnName);
	}

	@Override
	public void insert(Param... aParams) throws JDOException {
		if (DebugFile.trace) {
			StringBuffer oParams = new StringBuffer();
			if (aParams!=null)
				for (Param p : aParams)
					oParams.append(",").append(p.getName()).append("=").append(p.getValue());
			DebugFile.writeln("Begin JDBCTable.insert({"+oParams.toString().substring(1)+"})");
			DebugFile.incIdent();
		}

		StringBuffer oCols = new StringBuffer();
		for (Param v : aParams)
			oCols.append(",").append(v.getName());
		PreparedStatement oStmt = null;			  
		try {
			String sSQL = "INSERT INTO "+name()+" ("+oCols.substring(1)+") VALUES (?"+String.format(String.format("%%0%dd",aParams.length-1),0).replace("0",",?")+")";
			if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement("+sSQL+")");
			oStmt = getConnection().prepareStatement(sSQL);
			int p = 0;
			for (Param v : aParams)
				oStmt.setObject(++p, v.getValue(), v.getType());
			oStmt.executeUpdate();
			oStmt.close();
			oStmt=null;
		} catch (SQLException sqle) {
			if (DebugFile.trace) {
				DebugFile.writeln("SQLException "+sqle.getMessage());
				DebugFile.decIdent();
			}	  
			try { if (oStmt!=null) oStmt.close(); } catch (Exception ignore) { }
			throw new JDOException(sqle.getMessage(), sqle);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCTable.insert()");
		}	  
	}
	
	@Override
	public int update(Param[] aValues, AbstractQuery oQry) throws JDOException {
		int iAffected = 0;
		if (oQry==null) throw new NullPointerException("JDBCIndexableTable.update() filter query cannot be null");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCIndexableTable.update()");
			DebugFile.incIdent();
		}

		if (aValues!=null) {
			if (aValues.length>0) {
				StringBuffer oVals = new StringBuffer();
				for (Param v : aValues) {
					Object oVal = v.getValue();
					if (oVal instanceof LatLong) {
						LatLong oLl = (LatLong) oVal; 
						oVals.append(",").append(v.getName()).append("=ST_GeographyFromText('SRID=4326;POINT("+String.valueOf(oLl.getLattitude())+" "+String.valueOf(oLl.getLongitude())+")')");
					} else {
						oVals.append(",").append(v.getName()).append("=?");
					}
				}
				PreparedStatement oStmt = null;
				String sSQL = "UPDATE "+name()+" SET "+oVals.substring(1)+" WHERE "+oQry.getFilter();
				try {
					if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement("+sSQL+")");
					oStmt = getConnection().prepareStatement(sSQL);
					int p = 0;
					for (Param v : aValues) {
						Object oVal = v.getValue();
						if (oVal==null)
							oStmt.setNull(++p, v.getType());
						else if (!(oVal instanceof LatLong))
							oStmt.setObject(++p, v.getValue(), v.getType());
					}
					((SQLQuery) oQry).setParameters(oStmt);
					iAffected = oStmt.executeUpdate();
					oStmt.close();
					oStmt=null;
				} catch (SQLException sqle) {
					if (DebugFile.trace) {
						DebugFile.writeln("SQLException "+sqle.getMessage()+" "+sSQL);
						DebugFile.decIdent();
					}
					try { if (oStmt!=null) oStmt.close(); } catch (Exception ignore) { }
					throw new JDOException(sqle.getMessage(), sqle);
				}
			}
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCIndexableTable.update() : "+String.valueOf(iAffected));
		}

		return iAffected;
	}

	@Override
	public int update(Param[] values, Param[] params) throws JDOException {
		SQLQuery qry = new SQLQuery(this);
		Predicate where = qry.newPredicate(Connective.AND);
		try {
			for (Param p : params)
				where.add(p.getName(), Operator.EQ, p.getValue());
		} catch (UnsupportedOperationException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException xcpt) {
			throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
		}
		return update(values, qry);
	}
	
	@Override
	public int delete(AbstractQuery oQry) throws JDOException {
		int iAffected = 0;
		if (oQry==null) throw new NullPointerException("JDBCIndexableTable.delete() filter query cannot be null");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCIndexableTable.delete()");
			DebugFile.incIdent();
		}

				PreparedStatement oStmt = null;
				String sSQL = "DELETE FROM "+name()+" WHERE "+oQry.getFilter();
				try {
					if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement("+sSQL+")");
					oStmt = getConnection().prepareStatement(sSQL);
					((SQLQuery) oQry).setParameters(oStmt);
					iAffected = oStmt.executeUpdate();
					oStmt.close();
					oStmt=null;
				} catch (SQLException sqle) {
					if (DebugFile.trace) {
						DebugFile.writeln("SQLException "+sqle.getMessage()+" "+sSQL);
						DebugFile.decIdent();
					}
					try { if (oStmt!=null) oStmt.close(); } catch (Exception ignore) { }
					throw new JDOException(sqle.getMessage(), sqle);
				}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCIndexableTable.delete() : "+String.valueOf(iAffected));
		}
		return iAffected;
	}
	
	@Override
	public int delete(Param[] where) throws JDOException {
		SQLQuery oQry = new SQLQuery(this);		
	    oQry.setFilter(oQry.newPredicate(Connective.AND, Operator.EQ, where));
		return delete(oQry);
	}

	@Override
	public void createIndex(String indexName, boolean unique, Using indexUsing, String... columns) throws JDOException {
		SQLIndex indexDef = new SQLIndex(getTableDef().getName(), indexName, columns, unique, indexUsing);
		Statement stmt = null;
		try {
			String ddl = indexDef.sqlScriptDef(RDBMS.valueOf(getConnection().getDataBaseProduct()));
			stmt = getConnection().createStatement();
			stmt.execute(ddl);
			stmt.close();
			stmt = null;
		} catch (SQLException sqle) {
			try { if (null!=stmt) stmt.close(); } catch (Exception ignore) { }
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}

	@Override
	public void dropIndex(String indexName) throws JDOException {
		String ddl;
		Statement stmt = null;
		try {
			RDBMS dbms = RDBMS.valueOf(getConnection().getDataBaseProduct());
			switch (dbms) {
				case ACCESS:
					ddl = "DROP INDEX " + indexName + " ON " + getTableDef().getName();
					break;
				case MSSQL:
					ddl = "DROP INDEX " + getTableDef().getName() + "." + indexName;
					break;
				case MYSQL:
					ddl ="ALTER TABLE " + getTableDef().getName() + " DROP INDEX " + indexName;
					break;
				default:
					ddl = "DROP INDEX " + indexName;
			}
			stmt = getConnection().createStatement();
			stmt.execute(ddl);
			stmt.close();
			stmt = null;
		} catch (SQLException sqle) {
			try { if (null!=stmt) stmt.close(); } catch (Exception ignore) { }
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}
	
}
