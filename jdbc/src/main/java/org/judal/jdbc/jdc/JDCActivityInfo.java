package org.judal.jdbc.jdc;

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

import java.util.ArrayList;
import java.util.Date;

import org.judal.jdbc.RDBMS;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import com.knowgate.debug.DebugFile;

/**
* @author Sergio Montoro Ten
* @version 1.0
*/
public class JDCActivityInfo {

private JDCProcessInfo[] aProcessInfo = null;
private JDCLockConflict[] aLocksInfo = null;

private void pgSqlActivity(Connection oConn) throws SQLException {
  Statement oStmt;
  ResultSet oRSet;
  String sSlct;
  
  if (DebugFile.trace) {
    DebugFile.writeln("Begin JDCActivityInfo.pgSqlActivity([Connection])");
    DebugFile.incIdent();
  }
  
  ArrayList<JDCProcessInfo> aPinfo = new ArrayList<JDCProcessInfo>();
  oStmt = oConn.createStatement();
  sSlct = "SELECT datid,datname,pid,usesysid,usename,query,query_start FROM pg_stat_activity";
  oRSet = oStmt.executeQuery(sSlct+" WHERE query NOT LIKE '"+sSlct+"%'");
  while (oRSet.next()) {
    Date oDt;
    Timestamp oTs = oRSet.getTimestamp(7);
    if (oRSet.wasNull() || oTs==null) 
      oDt = null;
    else
    	oDt = new Date(oTs.getTime());
    aPinfo.add (new JDCProcessInfo(oRSet.getString(1),oRSet.getString(2),
                                   oRSet.getString(3),oRSet.getString(4),
                                   oRSet.getString(5),oRSet.getString(6), oDt));
  } // wend
  oRSet.close();
  oRSet = null;
  oStmt.close();
  oStmt=null;
  final int nProcs = aPinfo.size();
  if (nProcs>0) {
    aProcessInfo = new JDCProcessInfo[nProcs];
    aProcessInfo = aPinfo.toArray(aProcessInfo);
  } // fi

  ArrayList<JDCLockConflict> aLconfl = new ArrayList<JDCLockConflict>();
  oStmt = oConn.createStatement();
  oRSet = null;
  try {
    oRSet = oStmt.executeQuery("SELECT pid,waitingonpid,query,waitingonquery FROM v_querylockwait");
    while (oRSet.next()) {
      aLconfl.add (new JDCLockConflict(oRSet.getInt(1),oRSet.getInt(2),
                                     oRSet.getString(3),oRSet.getString(4)));
    } // wend
  } finally {
    if (null!=oRSet) oRSet.close();
    if (null!=oStmt) oStmt.close();
  }
  final int nLocks = aLconfl.size();
  if (nLocks>0) {
    aLocksInfo = new JDCLockConflict[nLocks];
    aLocksInfo = aLconfl.toArray(aLocksInfo);
  } // fi

  if (DebugFile.trace) {
    DebugFile.decIdent();
    DebugFile.writeln("End JDCActivityInfo.pgSqlActivity([Connection])");
  }

} // pgSqlActivity

public JDCActivityInfo(JDCConnection oConn) throws SQLException {
  if (oConn.getDataBaseProduct()==RDBMS.POSTGRESQL.intValue()) {
    pgSqlActivity(oConn);
    // running process
  } // fi
}

public JDCActivityInfo(JDCConnectionPool oPool)
  throws SQLException,ClassNotFoundException,NullPointerException {
  JDCConnection oConn = null;
  if (null==oPool)
    throw new NullPointerException("JDCActivityInfo() JDCConnectionPool parameter cannot be null");
  try {
    oConn = oPool.getConnection("activity_info");
    if (oConn.getDataBaseProduct()==RDBMS.POSTGRESQL.intValue()) {
      pgSqlActivity(oConn);
    } // fi
    oConn.close("activity_info");
    oConn=null;
  } finally {
    try { if (oConn!=null) oConn.close("activity_info"); } catch (Exception ignore) {} 
  }
}

public JDCProcessInfo[] processesInfo() {
  return aProcessInfo;
}

public JDCLockConflict[] lockConflictsInfo() {
  return aLocksInfo;
}

}
