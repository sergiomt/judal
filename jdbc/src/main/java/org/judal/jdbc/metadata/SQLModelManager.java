package org.judal.jdbc.metadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import java.util.regex.Pattern;

import org.judal.jdbc.RDBMS;
import org.judal.jdbc.jdc.JDCConnection;

import java.util.Properties;
import java.util.LinkedList;

import com.knowgate.debug.DebugFile;

/**
* <p>SQL Data Model Manager</p>
* <p>This class is used for creating a RDBMS data model from SQL scripts.</p>
* @author Sergio Montoro ten
*/

public abstract class SQLModelManager {

private static final String VERSION = "8.0.0";

protected static final int BULK_PROCEDURES = 1;
protected static final int BULK_STATEMENTS = 2;
protected static final int BULK_BATCH = 3;
protected static final int BULK_PLSQL = 4;
protected static final int FILE_STATEMENTS = 5;

private static final int CURRENT_TIMESTAMP_LEN = JDCConnection.CURRENT_TIMESTAMP.length();
private static final int DATETIME_LEN = JDCConnection.DATETIME.length();
private static final int LONGVARBINARY_LEN = JDCConnection.LONGVARBINARY.length();
private static final int LONGVARCHAR_LEN = JDCConnection.LONGVARCHAR.length();
private static final int FLOAT_NUMBER_LEN = JDCConnection.FLOAT_NUMBER.length();
private static final int NUMBER_6_LEN = JDCConnection.NUMBER_6.length();
private static final int NUMBER_11_LEN = JDCConnection.NUMBER_11.length();
private static final int CHARACTER_VARYING_LEN = JDCConnection.CHARACTER_VARYING.length();
private static final int SERIAL_LEN = JDCConnection.SERIAL.length();
private static final int BLOB_LEN = JDCConnection.BLOB.length();
private static final int CLOB_LEN = JDCConnection.CLOB.length();

private static final String CHAR_LENGTH = "LENGTH(";
private static final int CHAR_LENGTH_LEN = 7;

protected static final String CurrentTimeStamp[] = { null, "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP", "GETDATE()", null, "SYSDATE" };
protected static final String DateTime[] = { null, "TIMESTAMP", "TIMESTAMP", "DATETIME", null, "DATE" };
protected static final String LongVarChar[] = { null, "MEDIUMTEXT", "TEXT", "NTEXT", null, "LONG" };
protected static final String LongVarBinary[] = { null, "MEDIUMBLOB", "BYTEA", "IMAGE", null, "LONG RAW" };
protected static final String CharLength[] = { null, "CHAR_LENGTH(", "char_length(", "LEN(", null, "LENGTH(" };
protected static final String Serial[] = { null, "INTEGER NOT NULL AUTO_INCREMENT", "SERIAL", "INTEGER IDENTITY", null, "NUMBER(11)" };
protected static final String VarChar[] = { null, "VARCHAR", "VARCHAR", "NVARCHAR", null, "VARCHAR2" };
protected static final String Blob[] = { null, "MEDIUMBLOB", "BYTEA", "IMAGE", null, "BLOB" };
protected static final String Clob[] = { null, "MEDIUMTEXT", "TEXT", "NTEXT", null, "CLOB" };

protected String sDbms;
protected RDBMS eDbms;
protected String sSchema;

protected boolean bStopOnError;

protected String sEncoding;
protected boolean bASCII;

protected StringBuffer oStrLog;

private Connection oConn;

protected int iErrors;

// ---------------------------------------------------------------------------

protected class Constraint {
  public String constraintname;
  public String tablename;

  public Constraint(String sConstraintName, String sTableName) {
    constraintname = sConstraintName;
    tablename = sTableName;
  }
}

// ---------------------------------------------------------------------------

public SQLModelManager() {

  if (DebugFile.trace) {
    DebugFile.writeln("SQLModelManager build " + VERSION);
    DebugFile.envinfo();
  }

  eDbms = RDBMS.GENERIC;
  sDbms = null;
  oConn = null;
  oStrLog = null;
  bStopOnError = false;
  sEncoding = "UTF-8";
  bASCII = false;
}

// ---------------------------------------------------------------------------

public void activateLog(boolean bActivate) {
  oStrLog = (bActivate ? new StringBuffer() : null);      
}

// ---------------------------------------------------------------------------

public String getEncoding () {
  return sEncoding;
}

// ---------------------------------------------------------------------------

public void setEncoding (String sCharset) {
  sEncoding = sCharset;
}

// ---------------------------------------------------------------------------

/**
 * <p>Set whether or not create() and drop() methods should stop on error</p>
 * @param bStop <b>true</b>=stop on error, <b>false</b>=don not stop
 */
public void stopOnError(boolean bStop) {
  bStopOnError = bStop;
}

// ---------------------------------------------------------------------------

/**
 * <p>Get whether or not create() and drop() methods will stop on error</p>
 * @return bStop <b>true</b>=stop on error, <b>false</b>=don not stop
 */
public boolean stopOnError() {
  return bStopOnError;
}

// ---------------------------------------------------------------------------

/**
 * <p>Connect to database</p>
 * Connection autocommit is set to ON.
 * @param sDriver JDBC driver class name
 * @param sUrl Database URL
 * @param sUsr Database User
 * @param sPwd Database Password
 * @throws SQLException
 * @throws ClassNotFoundException If class for JDBC driver is not found
 * @throws IllegalStateException If already connected to database
 * @throws UnsupportedOperationException If DBMS is not recognized,
 * currently only Oracle, Microsoft SQL Server and PostgreSQL are recognized.
 */
public void connect(String sDriver, String sUrl, String sSch, String sUsr, String sPwd)
  throws SQLException, ClassNotFoundException, IllegalStateException, UnsupportedOperationException {

  if (DebugFile.trace) {
   DebugFile.writeln("Begin SQLModelManager.connect(" + sDriver + "," + sUrl + ", ...)");
   DebugFile.incIdent();
  }

  if (null!=oConn)
    throw new IllegalStateException("already connected to database");

  Connection oNewConn;
  Class oDriver = Class.forName(sDriver);

  oNewConn = DriverManager.getConnection(sUrl, sUsr, sPwd);
  oNewConn.setAutoCommit(true);

  setConnection(oNewConn);

  sSchema = sSch;

  if (DebugFile.trace) {
    DebugFile.decIdent();
    DebugFile.writeln("End SQLModelManager.connect()");
  }

} // connect

// ---------------------------------------------------------------------------

/**
 * <p>Disconnect from database</p>
 * @throws SQLException
 */
public void disconnect() throws SQLException {

  if (DebugFile.trace) {
   DebugFile.writeln("Begin SQLModelManager.disconnect()");
   DebugFile.incIdent();
  }

  if (null!=oConn) {
    if (!oConn.isClosed()) {
      oConn.close();
      sDbms = null;
    } // fi (!isClosed())
  } // fi (oConn)

  if (DebugFile.trace) {
    DebugFile.decIdent();
    DebugFile.writeln("End SQLModelManager.disconnect()");
  }
} // disconnect

// ---------------------------------------------------------------------------

/**
 * <p>Assign an external connection to SQLModelManager</p>
 * Use this method when ModelManager must not connect itself to database but
 * reuse an already existing connection.
 * @param oJDBCConn Database Connection
 * @throws SQLException
 * @throws UnsupportedOperationException If DBMS is not recognized
 * @throws NullPointerException if oJDBCConn is <b>null</b>
 */
public void setConnection(Connection oJDBCConn)
    throws SQLException,UnsupportedOperationException,NullPointerException {

  if (DebugFile.trace) {
   DebugFile.writeln("Begin SQLModelManager.setConnection([Connection])");
   DebugFile.incIdent();
  }

  if (null==oJDBCConn) throw new NullPointerException("Connection parameter may not be null");

  oConn = oJDBCConn;

  DatabaseMetaData oMDat = oConn.getMetaData();
  String sDatabaseProductName = oMDat.getDatabaseProductName();

  if (sDatabaseProductName.equals("Microsoft SQL Server")) {
    sDbms = "mssql";
    eDbms = RDBMS.MSSQL;
  }
  else if (sDatabaseProductName.equals("PostgreSQL")) {
    sDbms = "postgresql";
    eDbms = RDBMS.POSTGRESQL;
  }
  else if (sDatabaseProductName.equals("Oracle")) {
    sDbms = "oracle";
    eDbms = RDBMS.ORACLE;
  }
  else if (sDatabaseProductName.startsWith("DB2")) {
    sDbms = "db2";
    eDbms = RDBMS.DB2;
  }
  else if (sDatabaseProductName.startsWith("MySQL")) {
    sDbms = "mysql";
    eDbms = RDBMS.MYSQL;
  }
  else {
    sDbms = oMDat.getDatabaseProductName().toLowerCase();
    eDbms = RDBMS.UNKNOWN;
  }
  oMDat = null;

  if (eDbms.equals(RDBMS.GENERIC) || eDbms.equals(RDBMS.UNKNOWN)) {
    oConn.close();
    oConn = null;
    throw new UnsupportedOperationException("DataBase Management System not supported");
  }

  oStrLog = new StringBuffer();

  if (DebugFile.trace) {
    DebugFile.decIdent();
    DebugFile.writeln("End SQLModelManager.setConnection()");
  }
} // setConnection()

// ---------------------------------------------------------------------------

/**
 * Clear internal operation log
 */
public void clear() {
  if (null!=oStrLog) oStrLog.setLength(0);
}

// ---------------------------------------------------------------------------

/**
 * Get reference to opened database connection
 */
public Connection getConnection() {
  return oConn;
}

// ---------------------------------------------------------------------------

/**
 * Print internal operation log to a String
 */
public String report() {
  String sRep;

  if (null!=oStrLog)
    sRep = oStrLog.toString();
  else
    sRep = "";

  return sRep;
} // report

// ---------------------------------------------------------------------------

/**
 * <p>Translate SQL statement for a particular DBMS</p>
 * @param sSQL SQL to be translated
 * @throws NullPointerException if sSQL is <b>null</b>
 * @return SQL statement translated for the active DBMS
 */
public String translate(String sSQL)
  throws NullPointerException {

	  String sRetSql;
	
  if (DebugFile.trace) {
   DebugFile.writeln("Begin SQLModelManager.translate(" + sSQL + ")");
   DebugFile.incIdent();
  }

  if (null==sSQL) {
    if (DebugFile.trace) DebugFile.decIdent();
    throw new NullPointerException("Sentence to translate may not be null");
  }

  final int iLen = sSQL.length();

  if (iLen<=0) {
    if (DebugFile.trace) {
      DebugFile.decIdent();
      DebugFile.writeln("End SQLModelManager.translate()");
    }
    sRetSql = "";
  }
  else {
    int iPos, iOff;
    boolean bMatch;

    StringBuffer oTrn = new StringBuffer(iLen);

    for (int p=0; p<iLen; p++) {

      bMatch = true;
      for (iPos=0, iOff=p; iPos<CURRENT_TIMESTAMP_LEN && iOff<iLen && bMatch; iOff++, iPos++)
        bMatch = (sSQL.charAt(iOff) == JDCConnection.CURRENT_TIMESTAMP.charAt(iPos));

      if (bMatch) {

        oTrn.append(CurrentTimeStamp[eDbms.intValue()]);
        p += CURRENT_TIMESTAMP_LEN-1;
      }
      else {

        bMatch = true;
        for (iPos=0, iOff=p; iPos<DATETIME_LEN && iOff<iLen && bMatch; iOff++, iPos++)
          bMatch = (sSQL.charAt(iOff) == JDCConnection.DATETIME.charAt(iPos));

        if (bMatch) {

          oTrn.append(DateTime[eDbms.intValue()]);
          p += DATETIME_LEN-1;
        }
        else {

          if (p>0)
            bMatch = sSQL.charAt(p-1)!='N';
          else
            bMatch = true;

          for (iPos=0, iOff=p; iPos<CHARACTER_VARYING_LEN && iOff<iLen && bMatch; iOff++, iPos++)
            bMatch &= (sSQL.charAt(iOff) == JDCConnection.CHARACTER_VARYING.charAt(iPos));

          if (bMatch) {

            oTrn.append(VarChar[eDbms.intValue()]);
            p += CHARACTER_VARYING_LEN-1;
          }
          else {

            bMatch = true;
            for (iPos=0, iOff=p; iPos<LONGVARCHAR_LEN && iOff<iLen && bMatch; iOff++, iPos++)
              bMatch = (sSQL.charAt(iOff) == JDCConnection.LONGVARCHAR.charAt(iPos));

            if (bMatch) {

              oTrn.append(LongVarChar[eDbms.intValue()]);
              p += LONGVARCHAR_LEN-1;
            }
            else {

              bMatch = true;
              for (iPos=0, iOff=p; iPos<LONGVARBINARY_LEN && iOff<iLen && bMatch; iOff++, iPos++)
                bMatch = sSQL.charAt(iOff) == JDCConnection.LONGVARBINARY.charAt(iPos);

              if (bMatch) {

                oTrn.append(LongVarBinary[eDbms.intValue()]);
                p += LONGVARBINARY_LEN-1;
              }
              else {

                bMatch = true;
                for (iPos=0, iOff=p; iPos<CHAR_LENGTH_LEN && iOff<iLen && bMatch; iOff++, iPos++)
                  bMatch = sSQL.charAt(iOff) == CHAR_LENGTH.charAt(iPos);

                if (bMatch) {

                  oTrn.append(CharLength[eDbms.intValue()]);
                  p += CHAR_LENGTH_LEN-1;
                }

                else {
                  bMatch = true;
                  for (iPos = 0, iOff = p;
                       iPos < SERIAL_LEN && iOff < iLen && bMatch; iOff++, iPos++)
                    bMatch = sSQL.charAt(iOff) == JDCConnection.SERIAL.charAt(iPos);

                  if (bMatch) {

                    oTrn.append(Serial[eDbms.intValue()]);
                    p += SERIAL_LEN - 1;
                  }

                  else {

                    if (RDBMS.ORACLE.equals(eDbms)) {

                      bMatch = true;
                      for (iPos = 0, iOff = p;
                           iPos < NUMBER_6_LEN && iOff < iLen && bMatch; iOff++,
                           iPos++)
                        bMatch = sSQL.charAt(iOff) == JDCConnection.NUMBER_6.charAt(iPos);

                      if (bMatch) {

                        oTrn.append("NUMBER(6,0)");
                        p += NUMBER_6_LEN - 1;
                      }
                      else {

                        bMatch = true;
                        for (iPos = 0, iOff = p;
                             iPos < NUMBER_11_LEN && iOff < iLen && bMatch;
                             iOff++, iPos++)
                          bMatch = sSQL.charAt(iOff) == JDCConnection.NUMBER_11.charAt(iPos);

                        if (bMatch) {

                          oTrn.append("NUMBER(11,0)");
                          p += NUMBER_11_LEN - 1;
                        }
                        else {

                          bMatch = true;
                          for (iPos = 0, iOff = p;
                               iPos < FLOAT_NUMBER_LEN && iOff < iLen && bMatch;
                               iOff++, iPos++)
                            bMatch = sSQL.charAt(iOff) ==
                            		JDCConnection.FLOAT_NUMBER.charAt(iPos);

                          if (bMatch) {

                            oTrn.append("NUMBER");
                            p += FLOAT_NUMBER_LEN - 1;
                          }
                          else {
                            oTrn.append(sSQL.charAt(p));
                          }
                        } // // fi (NUMBER(11))

                      } // fi (NUMBER(6))

                    } // fi (DBMS_ORACLE)

                    else {

                      bMatch = true;
                      for (iPos = 0, iOff = p;
                           iPos < BLOB_LEN && iOff < iLen && bMatch; iOff++,
                           iPos++)
                        bMatch = sSQL.charAt(iOff) == JDCConnection.BLOB.charAt(iPos);

                      if (bMatch) {
                        oTrn.append(Blob[eDbms.intValue()]);
                        p += BLOB_LEN - 1;
                      }
                      else {
                        bMatch = true;
                        for (iPos = 0, iOff = p;
                             iPos < CLOB_LEN && iOff < iLen && bMatch; iOff++,
                             iPos++)
                          bMatch = sSQL.charAt(iOff) == JDCConnection.CLOB.charAt(iPos);

                        if (bMatch) {
                          oTrn.append(Clob[eDbms.intValue()]);
                          p += CLOB_LEN - 1;
                        }
                        else
                          oTrn.append(sSQL.charAt(p));
                      }
                    }
                  }
                }
              } // fi (no matching translation found)
            }
          }
        }
      }
    } // next

    if (bASCII) {
      int iTrn = oTrn.length();
      StringBuffer oAsc = new StringBuffer(iTrn);
      char[] cTrn = new char[1];
      for (int i=0; i<iTrn; i++) {
        oTrn.getChars(i, i + 1, cTrn, 0);
        if ((int)cTrn[0] <= 255)
          oAsc.append(cTrn[0]);
        else
          oAsc.append('?');
      } // next
      sRetSql = oAsc.toString().replace((char)13, (char)32);
    }
    else {
      sRetSql = oTrn.toString().replace((char)13, (char)32);
    } // fi (iLen)
  }

	  if (eDbms==RDBMS.MYSQL) {
	  Pattern oPatt = Pattern.compile(" DROP CONSTRAINT ", Pattern.CASE_INSENSITIVE);
	  sRetSql = oPatt.matcher(sRetSql).replaceAll(" DROP FOREIGN KEY ");

	    if (sRetSql.toUpperCase().startsWith("DROP INDEX ")) {
	  	  int iDot = sRetSql.indexOf('.');
	  	  if (iDot>0)
	  	    sRetSql = "DROP INDEX "+sRetSql.substring(iDot+1)+" ON "+sRetSql.substring(11,iDot);
	    }
	  } // fi

  if (DebugFile.trace) {
    DebugFile.decIdent();
    DebugFile.writeln("End SQLModelManager.translate() :\n" + sRetSql + "\n");
  }
	return sRetSql;
} // translate

// ---------------------------------------------------------------------------

private boolean isDoubleQuote(StringBuffer oBuffer, int iLen, int iPos) {
  boolean bRetVal;
	if (iPos>=iLen-2)
    bRetVal = false;
  else
  	bRetVal = (oBuffer.charAt(++iPos)==(char)39);    
	if (DebugFile.trace) DebugFile.writeln("found double quoute at position "+String.valueOf(iPos));
	return bRetVal;
} // isDoubleQuote

// ---------------------------------------------------------------------------

private boolean switchQuoteActiveStatus (StringBuffer oBuffer, int iLen, char cAt, int iPos, boolean bActive) {
  boolean bRetVal;
  // If a single quote sign ' is found then switch ON or OFF the value of bActive
  if (cAt==39) {
    if (isDoubleQuote(oBuffer, iLen, iPos))
    	bRetVal = bActive;
    else
    	bRetVal = !bActive;
  } else {
    bRetVal = bActive;
  }// fi (cAt==')
  if (DebugFile.trace && bRetVal!=bActive) {
    String sLine = "";
    for (int l=iPos; l<iLen && oBuffer.charAt(l)>=32; l++) sLine += oBuffer.charAt(l);
	    DebugFile.writeln("switching quote status to "+String.valueOf(bRetVal)+" at character position " + String.valueOf(iPos)+ " near "+sLine);
  }
  return bRetVal;
} // switchQuoteActiveStatus

// ---------------------------------------------------------------------------

private String[] split(StringBuffer oBuffer, char cDelimiter, String sGo) {

  // Fast String splitter routine specially tuned for SQL sentence batches
  if (DebugFile.trace) {
    DebugFile.writeln("Begin SQLModelManager.split([StringBuffer], "+cDelimiter+", "+sGo+")");
    DebugFile.incIdent();
  }

  final int iLen = oBuffer.length();
  int iGo;

  if (null!=sGo)
    iGo = sGo.length();
  else
    iGo = 0;

  char cAt;

  // Initially bActive is set to true
  // bActive signals that the current status is sensitive
  // to statement delimiters.
  // When a single quote is found, bActive is set to false
  // and then found delimiters are ignored until another
  // matching closing quote is reached.
  boolean bActive = true;
  int iStatementsCount = 0;
  int iMark = 0, iTail = 0, iIndex = 0, iQuote = 0;

  // Scan de input buffer
  for (int c=0; c<iLen; c++) {
    cAt = oBuffer.charAt(c);
    
    if (iGo>0 && RDBMS.POSTGRESQL.equals(eDbms)) {
    	boolean bSwitched = switchQuoteActiveStatus(oBuffer, iLen, cAt, c, bActive);
      if (!bSwitched) iQuote = c;
    	bActive = bSwitched;
      if (c<iLen-1) if ((cAt==(char)39) && (oBuffer.charAt(c+1)==(char)39)) c++;
    }

    // If the statement delimiter is found outside a quoted text then count a new line
    if (cAt==cDelimiter && bActive) {
      if (null==sGo) {
        iStatementsCount++;
      } else if (c>=iGo) {
        if (oBuffer.substring(c-iGo,c).equalsIgnoreCase(sGo)) {
          iStatementsCount++;
          if (DebugFile.trace) DebugFile.writeln("statement delimiter " + String.valueOf(iStatementsCount) + " found at character position "+c);
        }
      }
    } // fi (cAt==cDelimiter && bActive)
    // Skip any blank or non-printable characters after the end-of-statement marker
    for (iMark=c+1; iMark<iLen; iMark++)
      if (oBuffer.charAt(iMark)>32) break;
  } // next (c)

  if (DebugFile.trace && !bActive) {
  	int iEnd = iQuote+20;
  	if (iEnd>iLen) iEnd=iLen;
  	iQuote -= 20;
  	if (iQuote<0) iQuote=0;    	
  	DebugFile.writeln("Error: parsing finished without finding a match for quote near "+oBuffer.substring(iQuote,iEnd));
  }

  String aArray[] = new String[iStatementsCount];
  iMark  = iTail = iIndex = 0;
  bActive = true;
  for (int c=0; c<iLen; c++) {
    cAt = oBuffer.charAt(c);

    if (iGo>0 && RDBMS.POSTGRESQL.equals(eDbms)) {
      bActive = switchQuoteActiveStatus(oBuffer, iLen, cAt, c, bActive);
      if (c<iLen-1) if ((cAt==(char)39) && (oBuffer.charAt(c+1)==(char)39)) c++;
    }

    // If reached and end-of-statement marker outside a quoted text
    // and either there is no "GO" marker
    // or the "GO" marker is just prior to the delimiter
    if ((cAt==cDelimiter && bActive) &&
  	  (null==sGo || (c>=iGo && oBuffer.substring(c-iGo,c).equalsIgnoreCase(sGo)))) {

  	// Scan backwards from the end-of-statement
      for ( iTail=c-1; iTail>0; iTail--) {
        // If there is no "GO" then just skip blank spaces between the end-of-statement marker
        // and the last printable character of the statement
        if (oBuffer.charAt(iTail)>32 && null==sGo)
          break;
        else
      	// Just step back the length of the "GO" marker and break
      	if (null!=sGo) {
            iTail -= iGo;
            break;
          }
      } // next

      try {
        // Assign the statement to an array line
        aArray[iIndex] = oBuffer.substring(iMark,iTail+1);
        iIndex++;
      } catch (ArrayIndexOutOfBoundsException aioobe) {
        String sXcptInfo = aioobe.getMessage()+" c="+String.valueOf(c)+" at="+cAt+" active="+String.valueOf(bActive)+" aArray.length="+String.valueOf(iStatementsCount)+" oBuffer.length="+String.valueOf(oBuffer.length())+" iIndex="+String.valueOf(iIndex)+" iMark="+String.valueOf(iMark)+" iTail="+String.valueOf(iTail);
        if (iIndex>0) sXcptInfo += " next to " + aArray[iIndex-1];
        throw new ArrayIndexOutOfBoundsException(sXcptInfo);
      }

      // Skip any blank or non-printable characters after the end-of-statement marker
      for (iMark=c+1; iMark<iLen; iMark++)
        if (oBuffer.charAt(iMark)>32) break;

    } // fi (found delimiter)
  } // next (c)

  if (iIndex<iStatementsCount-1 && iMark<iLen-1)
    aArray[iIndex] = oBuffer.substring(iMark);

  if (DebugFile.trace) {
    DebugFile.decIdent();
    DebugFile.writeln("End SQLModelManager.split() : "+String.valueOf(iStatementsCount));
  }

  return aArray;
} // split

// ---------------------------------------------------------------------------

/**
 * Truncate table
 * @param sTableName String Table name
 * @throws SQLException
 * @since 6.0
 */

public void truncate (String sTableName) throws SQLException {
  Statement oStmt = getConnection().createStatement();
  oStmt.execute("TRUNCATE TABLE "+sTableName);
  oStmt.close();
  if (!getConnection().getAutoCommit()) getConnection().commit();
}

// ---------------------------------------------------------------------------

public void executeSQLScript (String sScriptSource, String sDelimiter)
  throws SQLException,InterruptedException,IllegalArgumentException {

  if (DebugFile.trace) {
    DebugFile.writeln("Begin SQLModelManager.executeSQLScript(String, "+sDelimiter+")");
    DebugFile.incIdent();
  }

  String sSQL;
  String aStatements[];

  if (sDelimiter.equals("GO;"))
    aStatements = split (new StringBuffer(sScriptSource), ';', "GO");
  else
    aStatements = split (new StringBuffer(sScriptSource), sDelimiter.charAt(0), null);

  int iStatements = aStatements.length;

  Statement oStmt = oConn.createStatement();

  for (int s = 0; s < iStatements; s++) {
    sSQL = aStatements[s];
    if (sSQL.length() > 0) {
      if (null!=oStrLog) oStrLog.append(sSQL + "\n\\\n");

      try {
        oStmt.execute (sSQL);
      }
      catch (SQLException sqle) {
        iErrors++;
        if (null!=oStrLog) oStrLog.append("SQLException: " + sqle.getMessage() + "\n");

        if (bStopOnError) {
          try { if (null!=oStmt) oStmt.close(); } catch (SQLException ignore) { }
          throw new InterruptedException(sqle.getMessage() + " " + sSQL);
        }
      }
    } // fi (sSQL)
    aStatements[s] = null;
  } // next
  oStmt.close();
  oStmt = null;

  if (DebugFile.trace) {
    DebugFile.decIdent();
    DebugFile.writeln("End SQLModelManager.executeSQLScript()");
  }
}

// ---------------------------------------------------------------------------

private StringBuffer getSQLBuffer(String sResourcePath, int iBatchType)
  throws FileNotFoundException, IOException,SQLException,InterruptedException {

    if (DebugFile.trace) {
      DebugFile.writeln("Begin SQLModelManager.getSQLBuffer(" + sResourcePath + ")");
      DebugFile.incIdent();
    }

    int iReaded, iSkip;
    final int iBufferSize = 16384;
    char Buffer[] = new char[iBufferSize];
    InputStream oInStrm;
    InputStreamReader oStrm;
    StringBuffer oBuffer = new StringBuffer();

    iErrors = 0;

    if (FILE_STATEMENTS == iBatchType) {
      if (DebugFile.trace) DebugFile.writeln("new FileInputStream("+sResourcePath+")");
      if (null!=oStrLog) oStrLog.append("Open file " + sResourcePath + " as " + sEncoding + "\n");
      oInStrm = new FileInputStream(sResourcePath);
    }
    else {
      if (DebugFile.trace) DebugFile.writeln(getClass().getName()+".getResourceAsStream("+sResourcePath+")");
      if (null!=oStrLog) oStrLog.append("Get resource " + sResourcePath + " as " + sEncoding + "\n");
      oInStrm = getClass().getResourceAsStream(sResourcePath);
    }

    if (null == oInStrm) {
      iErrors = 1;
      if (null!=oStrLog) oStrLog.append("FileNotFoundException "+sResourcePath);
      if (DebugFile.trace) {
        DebugFile.writeln("FileNotFoundException "+sResourcePath);
        DebugFile.decIdent();
      }
      throw new FileNotFoundException("executeBulk() " + sResourcePath);
    } // fi

    if (DebugFile.trace) DebugFile.writeln("new InputStreamReader([InputStream], "+sEncoding+")");

    oStrm = new InputStreamReader(oInStrm, sEncoding);

    try {
      while (true) {
        iReaded = oStrm.read(Buffer,0,iBufferSize);

        if (-1==iReaded) break;

        // Skip FF FE character mark for Unidode files
        iSkip = ((int)Buffer[0]==65279 || (int)Buffer[0]==65534 ? 1 : 0);

        oBuffer.append(Buffer, iSkip, iReaded-iSkip);

      }
      oStrm.close();
      oInStrm.close();
    }
    catch (IOException ioe) {
      iErrors = 1;
      if (null!=oStrLog) oStrLog.append("IOException "+ioe.getMessage());
      if (DebugFile.trace) DebugFile.decIdent();
      throw new IOException(ioe.getMessage());
    }

    if (DebugFile.trace) {
      DebugFile.decIdent();
      DebugFile.writeln("End SQLModelManager.getSQLBuffer() : " + String.valueOf(oBuffer.length()));
    }

    return oBuffer;
}

// ---------------------------------------------------------------------------

protected int executeBulk(StringBuffer oBuffer, String sResourcePath, int iBatchType)
  throws FileNotFoundException, IOException, SQLException,InterruptedException {

  if (DebugFile.trace) {
    DebugFile.writeln("Begin SQLModelManager.executeBulk(" + sResourcePath + "," +
                      String.valueOf(iBatchType) + ")");
    DebugFile.incIdent();
  }

  int iStatements;
  CallableStatement oCall = null;
  Statement oStmt = null;
  String sSQL = null;
  String aStatements[];

  iErrors = 0;

  if (sResourcePath.endsWith(".ddl") || sResourcePath.endsWith(".DDL"))
    aStatements = split(oBuffer, ';', "GO");
  else
    aStatements = split(oBuffer, ';', null);

  if (DebugFile.trace) DebugFile.writeln(String.valueOf(aStatements.length)+ " statements found");
  
  iStatements = aStatements.length;

    switch (iBatchType) {
      case BULK_PROCEDURES:
        for (int s = 0; s < iStatements; s++) {
          sSQL = aStatements[s];
          if (sSQL.length() > 0) {
            if (null!=oStrLog) oStrLog.append(sSQL + "\n");
            try {
              oCall = oConn.prepareCall(sSQL);
              oCall.execute();
              oCall.close();
              oCall = null;
            }
            catch (SQLException sqle) {
              iErrors++;
              if (null!=oStrLog) oStrLog.append("SQLException: " + sqle.getMessage() + "\n");
              try { if (null!=oCall) oCall.close(); } catch (SQLException ignore) { }
              if (bStopOnError) throw new java.lang.InterruptedException();
            }
          } // fi (sSQL)
        } // next
        break;

      case BULK_STATEMENTS:
      case FILE_STATEMENTS:
      case BULK_BATCH:

        oStmt = oConn.createStatement();
        for (int s = 0; s < iStatements; s++) {

          try {
            sSQL = translate(aStatements[s]);
          }
          catch (NullPointerException npe) {
            if (null!=oStrLog) oStrLog.append (" NullPointerException: at " + sResourcePath + " statement " + String.valueOf(s) + "\n");
            sSQL = "";
          }

          if (sSQL.length() > 0) {
            if (null!=oStrLog) oStrLog.append(sSQL + "\n\\\n");
            try {
          	if (!sSQL.startsWith("--")) {
                oStmt.executeUpdate(sSQL);
              }
            }
            catch (SQLException sqle) {
              iErrors++;
              if (null!=oStrLog) oStrLog.append ("SQLException: " + sqle.getMessage() + "\n");

              if (bStopOnError) {
                try { if (null!=oStmt) oStmt.close(); } catch (SQLException ignore) { }
                throw new java.lang.InterruptedException();
              }
            }
          } // fi (sSQL)
        } // next
        oStmt.close();
        oStmt = null;
        break;

      case BULK_PLSQL:
        oStmt = oConn.createStatement();
        for (int s = 0; s < iStatements; s++) {
          sSQL = aStatements[s];
          if (sSQL.length() > 0) {
            if (null!=oStrLog) oStrLog.append(sSQL + "\n\\\n");
            try {
            	if (DebugFile.trace) {
            		String s1stLine = sSQL.split("\n")[0];
            		DebugFile.writeln("Executing "+s1stLine);
            	}
              oStmt.execute(sSQL);
            }
            catch (SQLException sqle) {
              iErrors++;
              if (null!=oStrLog) oStrLog.append("SQLException: " + sqle.getMessage() + "\n");

              if (bStopOnError) {
                try { if (null!=oStmt) oStmt.close(); } catch (SQLException ignore) { }
                throw new java.lang.InterruptedException();
              }
            }
          } // fi (sSQL)
        } // next
        oStmt.close();
        oStmt = null;
        break;
/*
      case BULK_BATCH:
        oStmt = oConn.createStatement();
        for (int s = 0; s < iStatements; s++) {
          sSQL = aStatements[s];
          if (sSQL.length() > 0) {
              oStmt.addBatch(sSQL);
          } // fi (sSQL)
          int[] results = oStmt.executeBatch();
          for (int r=0; r<results.length; r++) {
            if (results[r]==1)
              if (null!=oStrLog) oStrLog.append(aStatements[r] + "\n\\\n");
            else {
              iErrors++;
              if (null!=oStrLog) oStrLog.append("ERROR: " + aStatements[r] + "\n\\\n");
              if (bStopOnError) {
                try { if (null!=oStmt) oStmt.close(); } catch (SQLException ignore) { }
                throw new java.lang.InterruptedException();
              }
            }
          }
        } // next
        oStmt.close();
        oStmt = null;
        break;
*/
    } // end switch()

  if (DebugFile.trace) {
    DebugFile.decIdent();
    DebugFile.writeln("End SQLModelManager.executeBulk()");
  }

  return iErrors;
} // executeBulk

// ---------------------------------------------------------------------------

protected int executeBulk(String sResourcePath, int iBatchType)
  throws FileNotFoundException, IOException, SQLException,InterruptedException {
  StringBuffer oBuffer = getSQLBuffer(sResourcePath, iBatchType);
  return executeBulk(oBuffer, sResourcePath, iBatchType);
}

// ---------------------------------------------------------------------------

protected StringBuffer changeSchema(String sResourcePath, int iType, String sOriginalSchema, String sNewSchema)
  throws InterruptedException, SQLException, IOException, FileNotFoundException {

  StringBuffer oBuffer = getSQLBuffer(sResourcePath, iType);
  String sBuffer = "";

  Pattern oPatt = Pattern.compile(sOriginalSchema+".", Pattern.CASE_INSENSITIVE);
  sBuffer = oPatt.matcher(oBuffer.toString()).replaceAll(sNewSchema+".");
  oBuffer = new StringBuffer(sBuffer);

  return oBuffer;
}

// ---------------------------------------------------------------------------

/**
 * <p>Create a functional module</p>
 * @param sModuleName Name of module to create
 * @return <b>true</b> if module was successfully created, <b>false</b> if errors
 * occured during module creation. Even if error occur module may still be partially
 * created at database after calling create()
 * @throws IllegalStateException If not connected to database
 * @throws FileNotFoundException If any of the internal files for module is not found
 * @throws SQLException
 * @throws IOException
 */
public abstract boolean create(String sModuleName)
  throws IllegalStateException, SQLException, FileNotFoundException, IOException;

// ---------------------------------------------------------------------------

/**
 * <p>Drop a functional module</p>
 * @param sModuleName Name of module to drop
 * @return <b>true</b> if module was successfully droped, <b>false</b> if errors
 * occured during droping module.
 * Even if error occur module may still be partially droped at database after calling drop()
 * @throws IllegalStateException
 * @throws SQLException
 * @throws FileNotFoundException
 * @throws IOException
 */
public abstract boolean drop(String sModuleName)
  throws IllegalStateException, SQLException, FileNotFoundException,IOException;

// ---------------------------------------------------------------------------

/**
 * <p>Create all modules</p>
 * @throws FileNotFoundException If any of the internal files for modules are not found
 * @throws IllegalStateException
 * @throws SQLException
 * @throws IOException
 */
public abstract boolean createAll()
  throws IllegalStateException, SQLException, FileNotFoundException, IOException;

// ---------------------------------------------------------------------------

/**
 * <p>Create a default database ready for use</p>
 * All modules for the full suite will be created at the new database.<br>
 * Error messages are written to internal ModelManager log and can be inspected by
 * calling report() method after createDefaultDatabase()
 * @throws Exception
 */

public abstract boolean createDefaultDatabase() throws Exception;

// ---------------------------------------------------------------------------

/**
 * <p>Drop all modules</p>
 * @throws IllegalStateException
 * @throws SQLException
 * @throws FileNotFoundException
 * @throws IOException
 */
public abstract boolean dropAll()
  throws IllegalStateException, SQLException, FileNotFoundException, IOException;

// ---------------------------------------------------------------------------

/**
 * Create INSERT SQL statements for the data of a table
 * @param sTableName Table Name
 * @param sWhere SQL filter clause
 * @param sFilePath Path for file where INSERT statements are to be written
 * @throws SQLException
 * @throws IOException
 */
public void scriptData (String sTableName, String sWhere, String sFilePath)
  throws SQLException, IOException {

  Statement oStmt;
  ResultSet oRSet;
  ResultSetMetaData oMDat;
  FileOutputStream oWriter;
  String sColumns;
  Object oValue;
  String sValue;
  String sValueEscaped;
  int iCols;
  byte[] byComma = new String(",").getBytes(sEncoding);
  byte[] byNull = new String("NULL").getBytes(sEncoding);
  byte[] byCRLF = new String(");\n").getBytes(sEncoding);

  oStmt = oConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  if (sWhere==null)
    oRSet = oStmt.executeQuery("SELECT * FROM " + sTableName + " ORDER BY 1");
  else
    oRSet = oStmt.executeQuery("SELECT * FROM " + sTableName + " WHERE " + sWhere + " ORDER BY 1");

  oMDat = oRSet.getMetaData();
  iCols = oMDat.getColumnCount();

  sColumns = "";

  for (int c=1; c<=iCols; c++) {
    if (!oMDat.getColumnName(c).equalsIgnoreCase("dt_created")) {
      if (c!=1) sColumns += ",";
      sColumns += oMDat.getColumnName(c);
    }
  } // next

  oWriter = new FileOutputStream(sFilePath);

  while (oRSet.next()) {
    sValue = "INSERT INTO " + sTableName + " (" + sColumns + ") VALUES (";

    oWriter.write(sValue.getBytes(sEncoding));

    for (int c=1; c<=iCols; c++) {

      if (!oMDat.getColumnName(c).equalsIgnoreCase("dt_created")) {

        if (c!=1) oWriter.write(byComma);

        switch (oMDat.getColumnType(c)) {

          case Types.CHAR:
          case Types.VARCHAR:
            sValue = oRSet.getString(c);
            if (oRSet.wasNull())
              sValueEscaped = "NULL";
            else if (sValue.indexOf(39)>=0) {
              sValueEscaped = "'";
              for (int n=0; n<sValue.length(); n++)
                sValueEscaped += (sValue.charAt(n)!=39 ? sValue.substring(n,n+1) : "''");
              sValueEscaped += "'";
            }
            else
              sValueEscaped = "'" + sValue + "'";
            oWriter.write(sValueEscaped.getBytes(sEncoding));
            break;

          case Types.SMALLINT:
            oValue = oRSet.getObject(c);
            if (oRSet.wasNull())
              oWriter.write(byNull);
            else
              oWriter.write(String.valueOf(oRSet.getShort(c)).getBytes(sEncoding));
            break;

          case Types.INTEGER:

            oValue = oRSet.getObject(c);
            if (oRSet.wasNull())
              oWriter.write(byNull);
            else
              oWriter.write(String.valueOf(oRSet.getInt(c)).getBytes(sEncoding));
            break;

          case Types.DATE:
          case Types.TIMESTAMP:
            oWriter.write(byNull);
            break;

        } // end switch
      } // fi (dt_created)
    } // next
    oWriter.write(byCRLF);
  } // wend

  oWriter.close();
} // scriptData

// ----------------------------------------------------------

/**
 * <p>Get an embedded resource file as a String</p>
 * @param sResourcePath Relative path at JAR file from com/knowgate/hipergate/datamodel/ModelManager
 * @param sEncoding Character encoding for resource if it is a text file.<br>
 * If sEncoding is <b>null</b> then UTF-8 is assumed.
 * @return Readed file
 * @throws FileNotFoundException
 * @throws IOException
 */
public String getResourceAsString (String sResourcePath, String sEncoding)
    throws FileNotFoundException, IOException {

  if (DebugFile.trace) {
    DebugFile.writeln("Begin SQLModelManager.getResourceAsString(" + sResourcePath + "," + sEncoding + ")");
    DebugFile.incIdent();
  }

  StringBuffer oXMLSource = new StringBuffer(12000);
  char[] Buffer = new char[4000];
  InputStreamReader oReader = null;
  int iReaded, iSkip;

  if (null==sEncoding) sEncoding = "UTF-8";

  InputStream oIoStrm = this.getClass().getResourceAsStream(sResourcePath);

	if (null==oIoStrm) throw new FileNotFoundException("Resource "+sResourcePath+" not found for class "+this.getClass().getName());

  oReader = new InputStreamReader(oIoStrm, sEncoding);
	
  while (true) {
    iReaded = oReader.read(Buffer, 0, 4000);

    if (-1==iReaded) break;

    // Skip FF FE character mark for Unidode files
    iSkip = ((int)Buffer[0]==65279 || (int)Buffer[0]==65534 ? 1 : 0);

    oXMLSource.append(Buffer, iSkip, iReaded-iSkip);
  } // wend

  oReader.close();
	oIoStrm.close();

  if (DebugFile.trace) {
    DebugFile.decIdent();
    DebugFile.writeln("End SQLModelManager.getResourceAsString()");
  }

  return oXMLSource.toString();

} // getResourceAsString

// ---------------------------------------------------------------------------

/**
* <p>Re-compile invalid objects for an Oracle database</p>
* @throws SQLException
* @throws FileNotFoundException
* @throws IOException
*/
public void recompileOrcl () throws SQLException {

  String sqlgencmd;
  Statement oStmt;
  CallableStatement oCall;
  ResultSet oRSet;
  String sAlterSql = "";

  if (DebugFile.trace) {
    DebugFile.writeln("Begin ModelManager.recompileOrcl()");
    DebugFile.incIdent();
  }

  // This SQL query produces the alter statements for recompile the objects which status is 'INVALID'
  sqlgencmd = " SELECT 'ALTER ' || DECODE(object_type, 'PACKAGE BODY', 'PACKAGE', object_type) || ' ' || ";
  sqlgencmd += "object_name || ' COMPILE' || DECODE(object_type, 'PACKAGE BODY', ' BODY', '') ";
  sqlgencmd += " cmd ";
  sqlgencmd += "FROM USER_OBJECTS ";
  sqlgencmd += "WHERE status = 'INVALID' AND ";
  sqlgencmd += "object_type IN ('TRIGGER','PACKAGE','PACKAGEBODY','VIEW','PROCEDURE','FUNCTION') AND ";
  sqlgencmd += "(object_type <> 'PACKAGE BODY' OR ";
  sqlgencmd += " (object_name) NOT IN ";
  sqlgencmd += "               (SELECT object_name ";
  sqlgencmd += "                FROM USER_OBJECTS ";
  sqlgencmd += "                WHERE object_type = 'PACKAGE' AND status = 'INVALID'))";

  if (null!=oStrLog) oStrLog.append(sqlgencmd+"\n");

  oStmt = oConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  oRSet = oStmt.executeQuery(sqlgencmd);

  while (oRSet.next()) {

    try {
         sAlterSql = oRSet.getString(1);
         oCall = oConn.prepareCall(sAlterSql);
         oCall.execute();
         oCall.close();

         if (null!=oStrLog) oStrLog.append(sAlterSql+"\n");
    }
    catch (SQLException sqle) {

         iErrors++;
         if (null!=oStrLog) oStrLog.append("SQLException: " + sqle.getMessage() + "\n");
         if (null!=oStrLog) oStrLog.append(sAlterSql + "\n");

         if (bStopOnError) {
           oRSet.close();
           oRSet = null ;
           oStmt.close();
           oStmt = null ;

           throw new SQLException("SQLException: " + sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode());
         }
    }

  } // wend
  if (null!=oRSet) oRSet.close();
  if (null!=oStmt) oStmt.close();

  if (DebugFile.trace) {
    DebugFile.decIdent();
    DebugFile.writeln("End ModelManager.recompileOrcl()");
  }
} // recompileOrcl

// ---------------------------------------------------------------------------

protected LinkedList<Constraint> listConstraints (Connection oJCon)
  throws SQLException {

  if (DebugFile.trace) {
    DebugFile.writeln("Begin SQLModelManager.listConstraints()");
    DebugFile.incIdent();
  }

  LinkedList<Constraint> oConstraintList = new LinkedList<>();
  Statement oStmt = oJCon.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  ResultSet oRSet = null;
  int iCount = 0;
  String sSQL = null;

  switch (eDbms) {
    case MSSQL:
      sSQL = "SELECT foreignkey.name AS constraintname,foreigntable.name AS tablename FROM sysforeignkeys sysfks, sysobjects foreignkey, sysobjects foreigntable WHERE sysfks.constid=foreignkey.id AND sysfks.fkeyid=foreigntable.id";
      break;
    case POSTGRESQL:
      sSQL = "SELECT c.conname,t.relname FROM pg_constraint c, pg_class t WHERE c.conrelid=t.oid AND c.contype='f'";
      break;
    case ORACLE:
      sSQL = "SELECT CONSTRAINT_NAME,TABLE_NAME FROM USER_CONSTRAINTS WHERE R_CONSTRAINT_NAME IS NOT NULL";
      break;
  }

  if (DebugFile.trace) DebugFile.writeln("Statement.executeQuery(" + sSQL + ")");

  oRSet = oStmt.executeQuery (sSQL);

  while (oRSet.next()) {
    oConstraintList.add (new Constraint(oRSet.getString(1),oRSet.getString(2)));
    iCount++;
  }

  oRSet.close();
  oStmt.close();

  if (DebugFile.trace) {
    DebugFile.decIdent();
    DebugFile.writeln("End SQLModelManager.listConstraints() : " + String.valueOf(iCount));
  }

  return oConstraintList;
} // listConstraints

// ----------------------------------------------------------

public void upgrade(String sOldVersion, String sNewVersion, Properties oProps)
  throws IllegalStateException, SQLException, FileNotFoundException, IOException {

} // upgrade

} // SQLModelManager
