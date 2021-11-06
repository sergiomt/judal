package org.judal.bdbj;

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

import com.sleepycat.je.Transaction;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.NonUniqueIndexDef;

import com.knowgate.debug.DebugFile;

/**
 * Subclass of com.knowgate.storage.Factory interface for Berkeley DB secondary indexes
 * @author Sergio Montoro Ten
 *
 */
public class DBIndex extends NonUniqueIndexDef {
  
	private SecondaryDatabase oSdb;

  private static Type getIndexTypeByName(String sRelationType) {
  	if (sRelationType.trim().equalsIgnoreCase("one-to-one"))
  		return Type.ONE_TO_ONE;
  	else if (sRelationType.trim().equalsIgnoreCase("many-to-one"))
  		return Type.MANY_TO_ONE;
  	else if (sRelationType.trim().equalsIgnoreCase("one-to-many"))
  		return Type.ONE_TO_MANY;
  	else if (sRelationType.trim().equalsIgnoreCase("many-to-many"))
  		return Type.MANY_TO_MANY;
  	else
  		throw new IllegalArgumentException("Unrecognized relation type "+sRelationType);  	
  }
  
  /**
   * 
   * @param sTableName String Table Name
   * @param sColumnName String Column Name
   * @param sRelationType String {"one-to-one","many-to-one","one-to-many","many-to-many"}
   * @throws IllegalArgumentException If relation type is not one of the four allowed values
   */
  public DBIndex(String sTableName, String sColumnName, String sRelationType)
    throws IllegalArgumentException {
	super(sTableName, sColumnName, new ColumnDef(sColumnName, Types.VARBINARY, 1), getIndexTypeByName(sRelationType));
  	oSdb = null;
  }
  
  public DBIndex(String sTableName, String sColumnName, Type eIndexType) {
	super(sTableName, sColumnName, new ColumnDef(sColumnName, Types.VARBINARY, 1), eIndexType);
  	oSdb = null;
  }
    
  public void open(SecondaryDatabase oSecDb) {
  	oSdb = oSecDb;
  }
  
  public void close() throws DatabaseException {
  	if (oSdb!=null) {
  	  oSdb.close();
  	  oSdb=null;
  	}
  }

  public boolean isClosed() {
    return oSdb==null;
  }
  
  public SecondaryCursor getCursor(Transaction oTrn) throws DatabaseException {
    if (DebugFile.trace) DebugFile.writeln("SecondaryDatabase.openSecondaryCursor(null,null)");
  	return oSdb.openSecondaryCursor(oTrn, null);
  }
  
}
