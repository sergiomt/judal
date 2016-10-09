package org.judal.jdbc;

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

import java.util.HashMap;

public enum RDBMS {

	UNKNOWN(-1),
	GENERIC(0),
	MYSQL(1),
	POSTGRESQL(2),
	MSSQL(3),
	SYBASE(4),
	ORACLE(5),
	DB2(6),
	INFORMIX(7),
	DERBY(8),
	XBASE(9),
	ACCESS(10),
	SQLITE(11),
	HSQLDB(12);
	
	private final int iCode;

	private static final HashMap<Integer,RDBMS> intToRdbmsMap = new HashMap<Integer,RDBMS>(21);
	
	static {
	  for (RDBMS db : RDBMS.values())
	    intToRdbmsMap.put(db.intValue(), db);	    
	}
	
	RDBMS (int iRDBMSCode) {
	  iCode = iRDBMSCode;
	}

	public String toString() {
	  switch (iCode) {
	    case 0: return "Generic DBMS";
	    case 1: return "MySQL";
	    case 2: return "PostgreSQL";
	    case 3: return "Microsoft SQL Server";
	    case 5: return "Oracle";
	    case 6: return "DB2";
	    case 9: return "XBase";
	    case 10: return "ACCESS";
	    case 11: return "SQLite";
	    case 12: return "HSQL Database Engine";
	    default: return "Unknown DBMS";
	  }
	}

	public String shortName() {
		  switch (iCode) {
		    case 0: return "generic";
		    case 1: return "mysql";
		    case 2: return "postgresql";
		    case 3: return "mssql";
		    case 5: return "oracle";
		    case 6: return "db2";
		    case 9: return "xbase";
		    case 10: return "access";
		    case 11: return "sqlite";
		    case 12: return "hsql";
		    default: return "unknown";
		  }
		}
	
	public final int intValue() {
	  return iCode;
	}

	public static RDBMS valueOf(int i) {
		RDBMS db = intToRdbmsMap.get(i);
	  return db==null ? UNKNOWN : db;
	}
	
}
