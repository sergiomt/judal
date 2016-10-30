package org.judal.bdb;

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

import java.sql.Types;
import java.util.Arrays;

import javax.jms.Message;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.IndexDef.Type;
import org.judal.metadata.TableDef;
import org.judal.storage.TableDataSource;
import org.judal.storage.ErrorCode;
import org.judal.storage.java.MapRecord;
import org.judal.storage.Table;
import com.knowgate.stringutils.Uid;

public class DBErrorLog extends MapRecord {
  
  private static final long serialVersionUID = 600000101201000111l;

  private static String K_ERRORS_LOG = "k_errors_log";
  
  private static DBErrorLog LogInstance = new DBErrorLog();
		  
  private static ColumnDef GU_ERROR = new ColumnDef(K_ERRORS_LOG, "gu_error", Types.CHAR, 32, 0, false, Type.ONE_TO_ONE, null, "GUID", true, 0);
  private static ColumnDef GU_ACCOUNT = new ColumnDef(K_ERRORS_LOG, "gu_account", Types.VARCHAR, 32, 0, false, Type.MANY_TO_ONE, null, null, false, 1);
  private static ColumnDef DT_CREATED = new ColumnDef(K_ERRORS_LOG, "dt_created", Types.TIMESTAMP, 19, 0, false, null, null, "NOW", false, 2);
  private static ColumnDef CO_ERROR   = new ColumnDef(K_ERRORS_LOG, "co_error"  , Types.INTEGER, 11, 0, false, null, null, "666", false, 3);
  private static ColumnDef BO_ACKNOWLEDGED = new ColumnDef(K_ERRORS_LOG, "bo_acknowledged", Types.BOOLEAN, 5, 0, false, null, null, "false", false, 4);
  private static ColumnDef TX_MESSAGE = new ColumnDef(K_ERRORS_LOG, "tx_message", Types.VARCHAR, 4000, 0, true, null, null, null, false, 5);
  private static ColumnDef JV_EXCEPTION = new ColumnDef(K_ERRORS_LOG, "jv_exception", Types.JAVA_OBJECT, 2147483647, 0, true, null, null, null, false, 6);
  private static ColumnDef JV_CAUSE = new ColumnDef(K_ERRORS_LOG, "jv_cause", Types.JAVA_OBJECT, 2147483647, 0, true, null, null, null, false, 7);
  private static ColumnDef JV_MESSAGE = new ColumnDef(K_ERRORS_LOG, "jv_message", Types.JAVA_OBJECT, 2147483647, 0, true, null, null, null, false, 8);

  public DBErrorLog() {
  	super(new TableDef(K_ERRORS_LOG, Arrays.asList(new ColumnDef[]{GU_ERROR,GU_ACCOUNT,DT_CREATED,CO_ERROR,BO_ACKNOWLEDGED,TX_MESSAGE,JV_EXCEPTION,JV_CAUSE,JV_MESSAGE})));
  }

  public String log (TableDataSource oDts, ErrorCode eCode, String sUid, String sTxMsg) {
	Table oTbl = null;
	try {
	  oTbl = oDts.openTable(LogInstance);
	  put("gu_error", Uid.createUniqueKey());
	  put("gu_account", sUid);
	  put("co_error", new Integer(eCode.intValue()));
	  put("tx_message", sTxMsg);
	  oTbl.store(this);
	  oTbl.close();
	} catch (Exception oHardXcpt) {
	  System.err.println("ErrorLog.log() "+oHardXcpt.getClass().getName()+" "+oHardXcpt.getMessage());
	}
	return getString("gu_error");
  }
  
  public String log (TableDataSource oDts, ErrorCode eCode, String sUid, Message oObjMsg,
  				     Exception oStorXcpt, Throwable oCauseXcpt) {
	Table oTbl = null;
	try {
	  oTbl = oDts.openTable(LogInstance);
	  put("gu_error", Uid.createUniqueKey());
	  put("gu_account", sUid);
	  put("co_error", new Integer(eCode.intValue()));
	  if (null!=oStorXcpt) {
	    put("tx_message", oStorXcpt.getMessage());
	    put("jv_exception", oStorXcpt);
	  }
	  if (null!=oCauseXcpt) put("jv_cause", oCauseXcpt);
	  if (null!=oObjMsg) put("jv_message", oObjMsg);
	  oTbl.store(this);
	  oTbl.close();
	} catch (Exception oHardXcpt) {
	  System.err.println("ErrorLog.log() "+oHardXcpt.getClass().getName()+" "+oHardXcpt.getMessage());
	}
	return getString("gu_error");
  }

}
