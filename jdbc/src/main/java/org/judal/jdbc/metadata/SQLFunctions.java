package org.judal.jdbc.metadata;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 */

import java.util.Map;

import org.judal.jdbc.RDBMS;

import com.knowgate.debug.DebugFile;

import java.util.HashMap;

/**
 * <p>Aliases for common SQL functions in different database dialects.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class SQLFunctions {

  public static Map<RDBMS,SQLFunctions> DB = init();
  
  private RDBMS rdbms;

  public SQLFunctions(RDBMS dbms) {
    setForDBMS(dbms);
  }

  public SQLFunctions(int dbmsId) {
	  setForDBMS(RDBMS.valueOf(dbmsId));
  }

  private static HashMap<RDBMS,SQLFunctions> init() {
    HashMap<RDBMS,SQLFunctions> dbs = new HashMap<RDBMS,SQLFunctions>(17);
    for (RDBMS rdbms : RDBMS.values()) {
    	try {
    		dbs.put(rdbms, new SQLFunctions(rdbms));
    	} catch (UnsupportedOperationException ignore) { }
    }
    return dbs;
  }

  /**
   * <p>ISNULL(value, default)</p>
   * Get value or default if value is null
   */
  public String ISNULL;

  /**
   * <p>String concatenation</p>
   * Str1 CONCAT Str2
   */
  public String CONCAT;

  /**
   * Get System Date
   */
  public String GETDATE;

  /**
   * <p>Transform String to lowercase</p>
   * LOWER(str)
   */
  public String LOWER;


  /**
   * <p>Transform String to uppercase</p>
   * UPPER(str)
   */
  public String UPPER;

  /**
   * <p>Get string length</p>
   * LENGTH(str)
   */
  public String LENGTH;

  /**
   * <p>n leftmost characters of a string</p>
   * LEFT(str, length)
   */
  public String LEFT;

  /**
   * <p>Get character from ASCII code</p>
   * CHAR([0..255])
   */
  public String CHR;

  /**
   * <p>Case-insensitve LIKE operator (PostgreSQL only)</p>
   */
  public String ILIKE;

  public int iDBMS;

  // -------------------------------------------------------------------------

  private void setForDBMS(RDBMS dbms) throws UnsupportedOperationException {

	if (DebugFile.trace) {
		DebugFile.writeln("Setting functions for "+dbms);
	}

	rdbms = dbms;

    if (dbms.equals(RDBMS.MSSQL)) {
      iDBMS = RDBMS.MSSQL.intValue();
      ISNULL = "ISNULL";
      CONCAT = "+";
      GETDATE = "GETDATE()";
      LOWER = "LOWER";
      UPPER = "UPPER";
      LENGTH = "LEN";
      CHR = "CHAR";
      ILIKE = "LIKE";
      LEFT = "LEFT";

    } else if (dbms.equals(RDBMS.ORACLE)) {
      iDBMS = RDBMS.ORACLE.intValue();
      ISNULL = "NVL";
      CONCAT = "||";
      GETDATE = "SYSDATE";
      LOWER = "LOWER";
      UPPER = "UPPER";
      LENGTH = "LENGTH";
      CHR = "CHR";
      ILIKE = "LIKE";
      LEFT = "SUBSTR";

    } else if (dbms.equals(RDBMS.POSTGRESQL)) {
      iDBMS = RDBMS.POSTGRESQL.intValue();
      ISNULL = "COALESCE";
      CONCAT = "||";
      GETDATE = "current_timestamp";
      LOWER = "lower";
      UPPER = "upper";
      LENGTH = "char_length";
      CHR = "chr";
      ILIKE = "ILIKE";
      LEFT = "LEFT";

    } else if (dbms.equals(RDBMS.MYSQL)) {
      iDBMS = RDBMS.MYSQL.intValue();
      ISNULL = "COALESCE";
      CONCAT = null; // MySQL uses CONCAT() function instead of an operator
      GETDATE = "NOW()";
      LENGTH = "CHAR_LENGTH";
      CHR = "CHAR";
      LOWER = "LCASE";
      UPPER = "UCASE";
      ILIKE = "LIKE";
      LEFT = "LEFT";

    } else if (dbms.equals(RDBMS.ACCESS)) {
      iDBMS = RDBMS.ACCESS.intValue();
      ISNULL = "NZ";
      CONCAT = "&";
      GETDATE = "NOW()";
      LENGTH = "LEN";
      CHR = "CHR";
      LOWER = "LCASE";
      UPPER = "UCASE";
      ILIKE = "LIKE";
      LEFT = "Left";

    } else if (dbms.equals(RDBMS.SQLITE)) {
      iDBMS = RDBMS.SQLITE.intValue();
      ISNULL = "coalesce";
      CONCAT = "||";
      GETDATE = "date('now')";
      LENGTH = "length";
      CHR = null; // SQLite does not have a CHR function
      LOWER = "lower";
      UPPER = "upper";
      ILIKE = "LIKE";
      LEFT = "SUBSTR";

    } else if (dbms.equals(RDBMS.XBASE)) {
      iDBMS = RDBMS.XBASE.intValue();
      ISNULL = "ISNULL";
      CONCAT = "+"; 
      GETDATE = "CURDATE()";
      LENGTH = "CHAR_LENGTH";
      CHR = "CHAR";
      LOWER = "LOWER";
      UPPER = "UPPER";
      ILIKE = "LIKE";
      LEFT = "LEFT";

    } else if (dbms.equals(RDBMS.HSQLDB)) {
        iDBMS = RDBMS.HSQLDB.intValue();
        ISNULL = "ISNULL";
        CONCAT = null; // HSQLDB uses CONCAT() function instead of an operator 
        GETDATE = "CURDATE()";
        LENGTH = "CHAR_LENGTH";
        CHR = "CHAR";
        LOWER = "LOWER";
        UPPER = "UPPER";
        ILIKE = "LIKE";
        LEFT = "LEFT";

    } else
      throw new UnsupportedOperationException("Unsupported DBMS " + dbms);

	if (DebugFile.trace) {
		DebugFile.writeln("Functions for "+dbms+" set");
	}

  } // setForDBMS

  // -------------------------------------------------------------------------

  public String apply(String func, String... values) throws IllegalArgumentException, UnsupportedOperationException {
	  if (func.equalsIgnoreCase(ISNULL)) {
		  if (values==null || values.length<1)
			  throw new IllegalArgumentException("SQLFunctions.apply("+ISNULL+", ...) needs one or more values");
		  StringBuilder expr = new StringBuilder();
		  expr.append(ISNULL).append("(").append(String.join(",", values)).append(")");
		  return expr.toString();
	  }
	  if (func.equalsIgnoreCase(LENGTH)) {
		  if (values==null || values.length!=1)
			  throw new IllegalArgumentException("SQLFunctions.apply("+LENGTH+", ...) needs exactly one value");
		  return LENGTH + "(" + values[0] + ")";
	  }
	  if (func.equalsIgnoreCase(LEFT)) {
			if (values==null || values.length!=2)
				throw new IllegalArgumentException("SQLFunctions.apply("+LEFT+", ...) needs exactly two values");
		  switch (rdbms) {
		  	case ORACLE:
		  	case SQLITE:
		  		return LEFT + "(" + values[0] + ",1," + values[1] + ")";
		  	default:
		  		return LEFT + "(" + values[0] + "," + values[1] + ")";
		  }
	  }
	  throw new UnsupportedOperationException("Unrecognized function "+func);
  }

  // -------------------------------------------------------------------------

  public String escape(java.util.Date dt, String sFormat) throws UnsupportedOperationException {
    String str;
    String sMonth, sDay, sHour, sMin, sSec;

    sMonth = (dt.getMonth()+1<10 ? "0" + String.valueOf((dt.getMonth()+1)) : String.valueOf(dt.getMonth()+1));
    sDay = (dt.getDate()<10 ? "0" + String.valueOf(dt.getDate()) : String.valueOf(dt.getDate()));
    sHour = (dt.getHours()<10 ? "0" + String.valueOf(dt.getHours()) : String.valueOf(dt.getHours()));
    sMin = (dt.getMinutes()<10 ? "0" + String.valueOf(dt.getMinutes()) : String.valueOf(dt.getMinutes()));
    sSec = (dt.getSeconds()<10 ? "0" + String.valueOf(dt.getSeconds()) : String.valueOf(dt.getSeconds()));

    if (iDBMS==RDBMS.MSSQL.intValue()) {

        str = "{ " + sFormat.toLowerCase() + " '";

        str += String.valueOf(dt.getYear()+1900) + "-" + sMonth + "-" + sDay + " ";

        if (sFormat.equalsIgnoreCase("ts")) {
          str += sHour + ":" + sMin +  ":" + sSec;
        }

        str = str.trim() + "'}";

    } else if (iDBMS==RDBMS.ORACLE.intValue()) {
        if (sFormat.equalsIgnoreCase("ts"))
          str = "TO_DATE('" + String.valueOf(dt.getYear()+1900) + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMin +  ":" + sSec + "','YYYY-MM-DD HH24-MI-SS')";
        else
          str = "TO_DATE('" + String.valueOf(dt.getYear()+1900) + "-" + sMonth + "-" + sDay + "','YYYY-MM-DD')";

    } else if (iDBMS==RDBMS.POSTGRESQL.intValue()) {
        if (sFormat.equalsIgnoreCase("ts"))
          str = "TIMESTAMP '" + String.valueOf(dt.getYear()+1900) + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMin +  ":" + sSec + "'";
        else
          str = "DATE '" + String.valueOf(dt.getYear()+1900) + "-" + sMonth + "-" + sDay + "'";

    } else if (iDBMS==RDBMS.MYSQL.intValue()) {
        if (sFormat.equalsIgnoreCase("ts"))
          str = "CAST('" + String.valueOf(dt.getYear()+1900) + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMin +  ":" + sSec + "' AS DATETIME)";
        else
          str = "CAST('" + String.valueOf(dt.getYear()+1900) + "-" + sMonth + "-" + sDay + "' AS DATE)";

    } else if (iDBMS==RDBMS.ACCESS.intValue()) {
        if (sFormat.equalsIgnoreCase("ts"))
          str = "CDate('" + sMonth + "/" + sDay + "/" + String.valueOf(dt.getYear()+1900) + " " + sHour + ":" + sMin +  ":" + sSec + "')";
        else
          str = "CDate('" + sMonth + "/" + sDay + "/" + String.valueOf(dt.getYear()+1900) + "')";

    } else if (iDBMS==RDBMS.XBASE.intValue()) {
        if (sFormat.equalsIgnoreCase("ts"))
          throw new UnsupportedOperationException("DBBind.Functions.escape(Date,String) unsupported casting to TIMESTAMP");
        else
          str = "'"+sMonth+"/"+sDay+"/"+String.valueOf(dt.getYear()+1900)+"'";

    } else if (iDBMS==RDBMS.HSQLDB.intValue()) {
        if (sFormat.equalsIgnoreCase("ts"))
          str = "TIMESTAMP ('" + String.valueOf(dt.getYear()+1900) + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMin +  ":" + sSec + "')";
        else
          str = "TO_DATE ('" + String.valueOf(dt.getYear()+1900) + "-" + sMonth + "-" + sDay + "', 'YYYY-MM-DD')";
        
    } else {
        throw new UnsupportedOperationException("DBBind.Functions.escape(Date,String) unsupported DBMS");
    } // end if

    return str;
  } // escape()

  // -------------------------------------------------------------------------

	/**
	 * Cast into CHARACTER VARYING SQL TYPE
	 * @param oData Object of any type
	 * @param iLength Maximum length of character data
	 * return <br/>
	 * For Oracle: TO_CHAR(oData)<br/>
	 * For MySQL: CAST(oData AS CHAR)<br/>
	 * For PostgreSQL and SQL Server: CAST(oData AS VARCHAR(iLength))<br/>
	 * For Access: CStr(oData)
	 */
  public String toChar(Object oData, int iLength) throws UnsupportedOperationException {
    String sRetVal;

    if (iDBMS==RDBMS.ORACLE.intValue()) {
        if (null==oData)
          sRetVal = "NULL";
        else	
          sRetVal = "TO_CHAR(" + oData.toString() + ")";

    } else if (iDBMS==RDBMS.MYSQL.intValue()) {
        if (null==oData)
          sRetVal = "NULL";
        else	
          sRetVal = "CAST(" + oData.toString() + " AS CHAR)";

    } else if (iDBMS==RDBMS.POSTGRESQL.intValue() || iDBMS==RDBMS.MSSQL.intValue() || iDBMS==RDBMS.HSQLDB.intValue()) {
        if (null==oData)
          sRetVal = "NULL";
        else	
          sRetVal = "CAST(" + oData.toString() + " AS VARCHAR(" + String.valueOf(iLength) + "))";

    } else if (iDBMS==RDBMS.ACCESS.intValue()) {

        if (null==oData)
          sRetVal = "NULL";
        else	
          sRetVal = "CStr(" + oData.toString() + ")";

    } else if (iDBMS==RDBMS.XBASE.intValue()) {
        if (null==oData)
          sRetVal = "NULL";
        else	
          sRetVal = oData.toString();

    } else {
        throw new UnsupportedOperationException("DBBind.Functions.toChar(Date,String) unsupported DBMS");
    }

    return sRetVal;
  } // toChar()

  /**
   * Create a SQL expressions which concatenates the given ones
   */
  public String strCat(String[] aExpressions, char cPlaceBetween) {
    String sRetExpr;
    if (null==aExpressions) {
      sRetExpr = null;
    } else if (aExpressions.length==0) {
      sRetExpr = "''";
    } else {
      if (iDBMS==RDBMS.MYSQL.intValue() || iDBMS==RDBMS.HSQLDB.intValue()) {
          sRetExpr = "CONCAT(";
          for (int e=0; e<aExpressions.length; e++) {
            sRetExpr += (0==e ? "" : ",") + ISNULL + "(" + aExpressions[e] + ",'')";
            if (cPlaceBetween!=0 && e<aExpressions.length-1)
              sRetExpr += ",'"+cPlaceBetween+"'";
          } // next
          sRetExpr += ")";
      } else {
          sRetExpr = "";
          for (int e=0; e<aExpressions.length; e++) {
            sRetExpr += (0==e ? "" : CONCAT) + ISNULL +"(" + aExpressions[e] + ",'')";
            if (cPlaceBetween!=0 && e<aExpressions.length-1)
              sRetExpr += CONCAT+"'"+cPlaceBetween+"'";
          } // next
      }
    } // fi
    return sRetExpr;
  } // strCat
  
  public String[] systemTables() {
	  if (RDBMS.ORACLE.intValue()==iDBMS)
	    return oracleSystemTables;
	    else if (RDBMS.POSTGRESQL.intValue()==iDBMS)
	    	return postgreSqlSystemTables;
	    else if (RDBMS.MSSQL.intValue()==iDBMS) 
	    	return sqlServerSystemTables;
	    else if (RDBMS.HSQLDB.intValue()==iDBMS) 
	    	return hsqldbSystemTables;
	    else
	    	return noSystemTables;
  }
  
  private static final String[] noSystemTables = new String[0];
		  
  private static final String[] oracleSystemTables = new String[]{ "AUDIT_ACTIONS", "STMT_AUDIT_OPTION_MAP", "DUAL",
	        "PSTUBTBL", "USER_CS_SRS", "USER_TRANSFORM_MAP", "CS_SRS", "HELP",
	        "SDO_ANGLE_UNITS", "SDO_AREA_UNITS", "SDO_DIST_UNITS", "SDO_DATUMS",
	        "SDO_CMT_CBK_DML_TABLE", "SDO_CMT_CBK_FN_TABLE", "SDO_CMT_CBK_DML_TABLE",
	        "SDO_PROJECTIONS", "SDO_ELLIPSOIDS", "SDO_GEOR_XMLSCHEMA_TABLE",
	        "SDO_GR_MOSAIC_0", "SDO_GR_MOSAIC_1", "SDO_GR_MOSAIC_2", "SDO_GR_MOSAIC_3",
	        "SDO_TOPO_RELATION_DATA", "SDO_TOPO_TRANSACT_DATA", "SDO_TXN_IDX_DELETES",
	        "DO_TXN_IDX_EXP_UPD_RGN", "SDO_TXN_IDX_INSERTS", "SDO_CS_SRS", "IMPDP_STATS",
	        "OLAP_SESSION_CUBES", "OLAP_SESSION_DIMS", "OLAPI_HISTORY",
	        "OLAPI_IFACE_OBJECT_HISTORY", "OLAPI_IFACE_OP_HISTORY", "OLAPI_MEMORY_HEAP_HISTORY",
	        "OLAPI_MEMORY_OP_HISTORY", "OLAPI_SESSION_HISTORY", "OLAPTABLEVELS","OLAPTABLEVELTUPLES",
	        "OLAP_OLEDB_FUNCTIONS_PVT", "OLAP_OLEDB_KEYWORDS", "OLAP_OLEDB_MDPROPS","OLAP_OLEDB_MDPROPVALS",
	        "OGIS_SPATIAL_REFERENCE_SYSTEMS", "SYSTEM_PRIVILEGE_MAP", "TABLE_PRIVILEGE_MAP" };
  
  private static final String[] postgreSqlSystemTables = new String[]{ "sql_languages", "sql_features",
			"sql_implementation_info", "sql_packages",
			"sql_sizing", "sql_sizing_profiles",
			"pg_ts_cfg", "pg_logdir_ls",
			"pg_ts_cfgmap", "pg_ts_dict", "pg_ts_parses",
			"pg_ts_parser", "pg_reload_conf", "spatial_ref_sys", "layer", "topology" };
  
  private static final String[] sqlServerSystemTables = new String[]{ "syscolumns", "syscomments", "sysdepends",
			"sysfilegroups", "sysfiles" , "sysfiles1",
			"sysforeignkeys", "sysfulltextcatalogs",
			"sysfulltextnotify", "sysindexes",
			"sysindexkeys", "sysmembers", "sysobjects",
			"syspermissions", "sysproperties",
			"sysprotects", "sysreferences", "systypes", "sysusers" };

  private static final String[] hsqldbSystemTables = new String[]{ "ADMINISTRABLE_ROLE_AUTHORIZATIONS","APPLICABLE_ROLES","ASSERTIONS",
		  "AUTHORIZATIONS","CHARACTER_SETS","CHECK_CONSTRAINTS","CHECK_CONSTRAINT_ROUTINE_USAGE","COLLATIONS","COLUMNS","COLUMN_COLUMN_USAGE",
		  "COLUMN_DOMAIN_USAGE","COLUMN_PRIVILEGES","COLUMN_UDT_USAGE","CONSTRAINT_COLUMN_USAGE","CONSTRAINT_TABLE_USAGE","DATA_TYPE_PRIVILEGES",
		  "DOMAINS","DOMAIN_CONSTRAINTS","ELEMENT_TYPES","ENABLED_ROLES","INFORMATION_SCHEMA_CATALOG_NAME","JARS","JAR_JAR_USAGE",
		  "KEY_COLUMN_USAGE","PARAMETERS","REFERENTIAL_CONSTRAINTS","ROLE_AUTHORIZATION_DESCRIPTORS","ROLE_COLUMN_GRANTS","ROLE_ROUTINE_GRANTS",
		  "ROLE_TABLE_GRANTS","ROLE_UDT_GRANTS","ROLE_USAGE_GRANTS","ROUTINES","ROUTINE_COLUMN_USAGE","ROUTINE_JAR_USAGE","ROUTINE_PRIVILEGES",
		  "ROUTINE_ROUTINE_USAGE","ROUTINE_SEQUENCE_USAGE","ROUTINE_TABLE_USAGE","SCHEMATA","SEQUENCES","SQL_FEATURES","SQL_IMPLEMENTATION_INFO",
		  "SQL_PACKAGES","SQL_PARTS","SQL_SIZING","SQL_SIZING_PROFILES","SYSTEM_BESTROWIDENTIFIER","SYSTEM_CACHEINFO","SYSTEM_COLUMNS",
		  "SYSTEM_COLUMN_SEQUENCE_USAGE","SYSTEM_COMMENTS","SYSTEM_CONNECTION_PROPERTIES","SYSTEM_CROSSREFERENCE","SYSTEM_INDEXINFO",
		  "SYSTEM_INDEXSTATS","SYSTEM_PRIMARYKEYS","SYSTEM_PROCEDURECOLUMNS","SYSTEM_PROCEDURES","SYSTEM_PROPERTIES","SYSTEM_SCHEMAS",
		  "SYSTEM_SEQUENCES","SYSTEM_SESSIONINFO","SYSTEM_SESSIONS","SYSTEM_SYNONYMS","SYSTEM_TABLES","SYSTEM_TABLESTATS","SYSTEM_TABLETYPES",
		  "SYSTEM_TEXTTABLES","SYSTEM_TYPEINFO","SYSTEM_UDTS","SYSTEM_USERS","SYSTEM_VERSIONCOLUMNS","TABLES","TABLE_CONSTRAINTS","TABLE_PRIVILEGES",
		  "TRANSLATIONS","TRIGGERED_UPDATE_COLUMNS","TRIGGERS","TRIGGER_COLUMN_USAGE","TRIGGER_ROUTINE_USAGE","TRIGGER_SEQUENCE_USAGE",
		  "TRIGGER_TABLE_USAGE","UDT_PRIVILEGES","USAGE_PRIVILEGES","USER_DEFINED_TYPES","VIEWS","VIEW_COLUMN_USAGE","VIEW_ROUTINE_USAGE",
		  "VIEW_TABLE_USAGE","BLOCKS","LOBS","LOB_IDS","PARTS"};
  
} // SQLFunctions
