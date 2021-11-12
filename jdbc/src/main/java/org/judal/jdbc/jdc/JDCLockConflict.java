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

/**
* @author Sergio Montoro Ten
* @version 1.0
*/
public class JDCLockConflict {

	private int iCurrentPID;
	private int iWaitingOnPID;
	private String sCurrentQuery;
	private String sWaitingOnQuery;

  public JDCLockConflict(int iPID, int iWaitOnPID, String sQry, String sWaitOnQry) {
		iCurrentPID = iPID;
		iWaitingOnPID = iWaitOnPID;
		sCurrentQuery = sQry;
		sWaitingOnQuery = sWaitOnQry;
  }

	public int getPID() {
		return iCurrentPID;
	}

	public int getWaitingOnPID() {
		return iWaitingOnPID;
	}

	public String getQuery() {
		return sCurrentQuery;
	}

	public String getWaitingOnQuery() {
		return sWaitingOnQuery;
	}

}
