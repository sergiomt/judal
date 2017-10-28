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

import java.io.IOException;

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
import java.util.HashMap;

import org.judal.jdbc.metadata.SQLIndex;
import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.metadata.IndexDef.Using;
import org.judal.storage.Param;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Connective;
import org.judal.storage.query.Expression;
import org.judal.storage.query.Operator;
import org.judal.storage.query.sql.SQLPredicate;
import org.judal.storage.query.sql.SQLQuery;
import org.judal.storage.relational.RelationalTable;
import org.judal.storage.table.Record;
import org.judal.storage.table.impl.AbstractRecord;

import javax.jdo.JDOException;
import javax.jdo.metadata.ColumnMetadata;
import javax.jdo.metadata.PrimaryKeyMetadata;

import com.knowgate.debug.DebugFile;
import com.knowgate.gis.LatLong;

/**
 * <p>JDBC relational table.</p>
 * Instances of this class are used to fetch, load, store and delete records from a relational table accessed through a JDBC data source.
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JDBCRelationalTable extends JDBCRelationalView implements RelationalTable {

	private SQLTableDef tableDef;
	
	/**
	 * <p>Constructor.</p>
	 * When using this constructor, the table with name as returned by recordInstance.getTableName() must exist in the data source schema metadata.
	 * @param dataSource JDBCTableDataSource
	 * @param recordInstance Record Instance of the Record implementation that will be used to fetch, load, store and delete records from the RDBMS table.
	 * @throws JDOException
	 */
	public JDBCRelationalTable(JDBCTableDataSource dataSource, Record recordInstance) throws JDOException {
		super(dataSource, recordInstance);
		tableDef = dataSource.getTableDef(recordInstance.getTableName());
	}

	/**
	 * <p>Constructor.</p>
	 * @param dataSource JDBCTableDataSource
	 * @param tableDef SQLTableDef
	 * @param recClass Class&lt;? extends Record&gt; Class of the Record implementation that will be used to fetch, load, store and delete records from the RDBMS table.
	 * @throws JDOException
	 */
	public JDBCRelationalTable(JDBCTableDataSource dataSource, SQLTableDef tableDef, Class<? extends Record> recClass) throws JDOException {
		super(dataSource, tableDef.asView(), recClass);
		this.tableDef = tableDef;
	}	
	
	/**
	 * <p>Close prepared statements and return the internal JDBC connection to the pool.</p>
	 * @throws JDOException
	 */
	@Override
	public void close() throws JDOException {
		if (preparedInsert!=null) {
			try {
				preparedInsert.close();
			} catch (SQLException sqle) {
				if (DebugFile.trace)
					DebugFile.writeln("JDBCRelationalTable.close() SQLException closing prepared insert statement "+sqle.getMessage());
			}
			preparedInsert = null;
		}
		super.close();
	}

	/**
	 * @return PrimaryKeyMetadata
	 */
	@Override
	public PrimaryKeyMetadata getPrimaryKey() {
		return tableDef.getPrimaryKeyMetadata();
	}
	
	/**
	 * @return SQLTableDef
	 */
	public SQLTableDef getTableDef() {
		return tableDef;
	}
	
	/**
	 * <p>Get name of the column which value will never be modified when store() call performs an update on a row of this table.</p> 
	 * @return String Column name or <b>null</b> if there is no timestamp column defined.
	 */
	@Override
	public String getTimestampColumnName() {
		return getTableDef().getCreationTimestampColumnName();
	}

	/**
	 * <p>Set name of the column which value will never be modified when store() call performs an update on a row of this table.</p> 
	 * @param String Column name or <b>null</b> if there is no timestamp column defined.
	 * @throws IllegalArgumentException
	 */
	@Override
	public void setTimestampColumnName(String columnName) throws IllegalArgumentException {
		getTableDef().setCreationTimestampColumnName(columnName);
	}

	private String paramSignature(Param... aParams) {
		StringBuilder sign = new StringBuilder();
		for (Param p : aParams)
			sign.append(String.valueOf(p.getType()));
		return sign.toString();
	}

	/**
	 * <p>Insert a new row in the database table accessed by this object.</p>
	 * @param aParams Param&hellip; Values to be inserted in the row. Each Param name must match a column name. There must be one Param matching each non  nullable column.
	 * @throws JDOException
	 */
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

		final String paramSign = paramSignature(aParams);		
		if (preparedInsert!=null && paramSign.equals(insertSignture)) {
			try {
				int p = 0;
				for (Param v : aParams)
					if (!(v.getValue() instanceof LatLong) && !(v.getValue() instanceof Expression))
						preparedInsert.setObject(++p, v.getValue(), v.getType());
				preparedInsert.executeUpdate();
			} catch (SQLException sqle) {
				if (DebugFile.trace) {
					DebugFile.writeln("SQLException "+sqle.getMessage());
					DebugFile.decIdent();
				}	  
				throw new JDOException(sqle.getMessage(), sqle);
			}			
		} else {
			if (preparedInsert!=null) {
				try {
					preparedInsert.close();
				} catch (SQLException sqle) {
					if (DebugFile.trace) DebugFile.writeln("Table.insert() SQLException closing PreparedStatement "+sqle.getMessage());
				}
				preparedInsert = null;
			}
			insertSignture = paramSign;
			StringBuilder oCols = new StringBuilder();
			for (Param v : aParams)
				oCols.append(",").append(v.getName());
			try {
				final String sSQL = "INSERT INTO "+name()+" ("+oCols.substring(1)+") VALUES (?"+String.format(String.format("%%0%dd",aParams.length-1),0).replace("0",",?")+")";
				if (DebugFile.trace) DebugFile.writeln("Connection.prepareStatement("+sSQL+")");
				preparedInsert = getConnection().prepareStatement(sSQL);
				int p = 0;
				for (Param v : aParams)
					if (!(v.getValue() instanceof LatLong) && !(v.getValue() instanceof Expression))
						preparedInsert.setObject(++p, v.getValue(), v.getType());
				preparedInsert.executeUpdate();
			} catch (SQLException sqle) {
				if (DebugFile.trace) {
					DebugFile.writeln("SQLException "+sqle.getMessage());
					DebugFile.decIdent();
				}	  
				throw new JDOException(sqle.getMessage(), sqle);
			}			
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCTable.insert()");
		}	  
	}
	
	/**
	 * <p>Update one or more rows in the database table accessed by this object.</p>
	 * @param aValues Param[] Values to be updated. Each Param name must match a column name.
	 * @param oQry AbstractQuery Query defining the rows to be updated.
	 * @throws JDOException
	 */
	@Override
	public int update(Param[] aValues, AbstractQuery oQry) throws JDOException {
		int iAffected = 0;
		if (oQry==null) throw new NullPointerException("JDBCIndexableTable.update() filter query cannot be null");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCIndexableTable.update(Param[], AbstractQuery)");
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
					} else if (oVal instanceof Expression) {
						oVals.append(",").append(v.getName()).append("=").append(oVal.toString());
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
						else if (!(oVal instanceof LatLong) && !(oVal instanceof Expression))
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

	/**
	 * <p>Update one or more rows in the database table accessed by this object.</p>
	 * @param values Param[] Values to be updated. Each Param name must match a column name.
	 * @param params Param[] Values used to filter rows to be updated. Matching rows must have values in all their columns as specified by params.
	 * @throws JDOException
	 */
	@Override
	public int update(Param[] values, Param[] params) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCRelationalTable.update(Param[], Param[])");
			DebugFile.incIdent();
		}
		
		SQLQuery qry = new SQLQuery(this);
		SQLPredicate where = qry.newPredicate(Connective.AND);
		try {
			for (Param p : params)
				where.add(p.getName(), Operator.EQ, p.getValue());
			qry.setFilter(where);
		} catch (UnsupportedOperationException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException xcpt) {
			throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
		}
		
		int retval = update(values, qry);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JDBCRelationalTable.update() : " + String.valueOf(retval));
		}
		
		return retval;
	}
	
	/**
	 * <p>Delete one or more rows.</p>
	 * @param oQry AbstractQuery Query defining the rows to be deleted.
	 * @throws JDOException
	 */
	@Override
	public int delete(AbstractQuery oQry) throws JDOException {
		int iAffected = 0;
		if (oQry==null) throw new NullPointerException("JDBCRelationalTable.delete() filter query cannot be null");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JDBCRelationalTable.delete()");
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
			DebugFile.writeln("End JDBCRelationalTable.delete() : "+String.valueOf(iAffected));
		}
		return iAffected;
	}
	
	/**
	 * <p>Delete one or more rows.</p>
	 * @param oQry AbstractQuery Query defining the rows to be deleted.
	 * @throws JDOException
	 */
	@Override
	public int delete(Param[] where) throws JDOException {
		SQLQuery oQry = new SQLQuery(this);		
	    oQry.setFilter(oQry.newPredicate(Connective.AND, Operator.EQ, where));
		return delete(oQry);
	}

	/**
	 * <p>Create an index on a table.</p>
	 * @param indexName String Index Name
	 * @param unique boolean Are indexed values unique?
	 * @param indexUsing Using Index type (if available) : BITMAP, BTREE, CLUSTERED, GIST.
	 * @param columns String&hellip; Indexed columns
	 * @throws JDOException
	 */
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

	/**
	 * <p>Drop an index.</p>
	 * @param indexName String Index Name
	 * @throws JDOException
	 */
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

	/**
	 * <p>Store Record into database.</p>
	 * Store will either update (if it already exists) or insert (if it does not exist) the value held by the Record instance.
	 * @param target Record
	 * @throws JDOException
	 */
	@Override
	public void store(Stored target) throws JDOException {
		boolean bHasLongVarBinaryData = false;
		AbstractRecord mapRecord = (AbstractRecord) target;
		
		if (null==target)
			throw new NullPointerException("JDBCBucket.store() Target instance may not be null");

		if (null==tableDef)
			throw new NullPointerException("JDBCBucket.store() Target TableDef may not be null");
		
		if (target instanceof AbstractRecord) {
			bHasLongVarBinaryData = ((AbstractRecord) target).hasLongData();
		}

		if (bHasLongVarBinaryData) {
			try {
				tableDef.storeRegisterLong(jdcConn, mapRecord, mapRecord.longDataLengths());
			} catch (IOException ioe) {
				throw new JDOException(ioe.getMessage(), ioe);
			} catch (SQLException sqle) {
				throw new JDOException(sqle.getMessage(), sqle);
			} finally {
				if (bHasLongVarBinaryData)
					mapRecord.clearLongData();
			}
		} else {
			try {
				tableDef.storeRegister(jdcConn, (AbstractRecord) target);
			} catch (SQLException sqle) {
				throw new JDOException(sqle.getMessage(), sqle);
			}
		}
	}

	/**
	 * <p>Delete row with given key.</p>
	 * @param key Object If key is instance of Param or Param[] then the values of the params will be used as key or keys
	 * @throws JDOException
	 * @throws ClassCastException
	 */
	@Override
	public void delete(Object key) throws JDOException, ClassCastException {
		PrimaryKeyMetadata pk = tableDef.getPrimaryKeyMetadata();
		if (pk.getNumberOfColumns()==0)
			throw new JDOException("Cannot delete a single record because table "+name()+" has no primary key");
		else if (pk.getNumberOfColumns()!=1 && !key.getClass().isArray())
			throw new JDOException("Not enough values supplied for primary key");
		else if (pk.getNumberOfColumns()>1 && ((Object[]) key).length!=pk.getNumberOfColumns())
			throw new JDOException("Wrong number of values supplied for primary key");
		HashMap<String,Object> keymap = new HashMap<String,Object>(5);
		if (pk.getNumberOfColumns()==1) {
			if (key instanceof Param)
				keymap.put(pk.getColumn(), key);
			else
				keymap.put(pk.getColumn(), ((Param) key).getValue());
		} else {
			Object[] keyvals;
			if (key instanceof Param[]) {
				Param[] params = (Param[]) key;
				keyvals = new Object[params.length];
				for (int p=0; p<params.length; p++)
					keyvals[p] = params[p].getValue();
			} else {
				keyvals = (Object[]) key;
			}
			if (keyvals.length!=pk.getNumberOfColumns())
				throw new JDOException("Wrong number of columns expected "+String.valueOf(pk.getNumberOfColumns())+" but got"+String.valueOf(keyvals.length));
			int k = 0;
			for (ColumnMetadata colDef: pk.getColumns())			
				keymap.put(colDef.getName(), keyvals[k++]);
		}
		try {
			tableDef.deleteRegister(jdcConn, keymap);
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
	}

	private String insertSignture = null;
	private PreparedStatement preparedInsert = null;

}
