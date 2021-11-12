package org.judal.jdbc.jdc;

/*
 * © Copyright 2016 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 */

import java.util.Date;

/**
* Information about a process serving a connection
* @author Sergio Montoro Ten
* @version 1.0
*/
public class JDCProcessInfo {

private String datid, datname, procpid, usesysid, usename, query_text;
private Date query_start;

protected JDCProcessInfo(String sDatId, String sDatName,
                      String sProcpId, String sUserSysId,
                      String sUserName, String sQueryText,
                      Date dtQueryStart) {
  datid=sDatId;
  datname=sDatName;
  procpid=sProcpId;
  usesysid=sUserSysId;
  usename=sUserName;
  query_text=sQueryText;
  query_start=dtQueryStart;
}

public String getProcessId() {
  return procpid;
}

public String getUserName() {
  return usename;
}

public String getQueryText() {
  return query_text==null ? "" : query_text;
}

public Date getQueryStart() {
  return query_start;
}

}

