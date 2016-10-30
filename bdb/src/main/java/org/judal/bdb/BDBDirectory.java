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

import java.io.IOException;
import java.util.HashMap;

import javax.transaction.TransactionManager;

import org.apache.lucene.store.db.DbDirectory;
import org.judal.storage.DataSource;

import com.sleepycat.db.Database;
import com.sleepycat.db.Transaction;

/**
 * Shared Berkeley DB Environment subclass of DbDirectory
 * @author Sergio Montoro Ten
 *
 */
public class BDBDirectory extends DbDirectory {
  private static DBBucketDataSource oDbEnv = null;
  private static int nClients = 0;
  private Transaction oDbTxn;
  
  private BDBDirectory(Transaction oTrns, Database oFiles, Database oBlks) {
    super(oTrns, oFiles, oBlks);
  }

  @Override
  public void close()
    throws IOException {
	super.close();
    try {
	  if (null!=oDbTxn)
	    oDbTxn.commit();
	  if (0==--nClients) {
	    if (null!=oDbEnv)
	      oDbEnv.close();
	    oDbEnv=null;
	  }
    } catch (Exception xcpt) {
      throw new IOException(xcpt.getMessage(),xcpt);
    }
  } 

  /**
   * <p>Open Lucene Directory on the specified disk path</p>
   * Two Berkely DB Database files will be opened with names
   * lucene_records and lucene_datablocks
   * @param sDirectory String Directory full path
   * @param oTrnMan TransactionManager
   * @return BDBDirectory
   * @throws IOException
   */
  public static BDBDirectory open(String sDirectory, TransactionManager oTrnMan)
    throws IOException {	
	BDBDirectory oDbDir = null;
	try {
	  if (null==oDbEnv) {
		  HashMap<String,String> oProps = new HashMap<String,String>();
		  oProps.put(DataSource.URI, sDirectory);
		  oDbEnv = new DBBucketDataSource(oProps, oTrnMan);
	  }
	  nClients++;
	  Transaction oTrns = oDbEnv.getEnvironment().beginTransaction(null,null);
	  Database oRecs = oDbEnv.openBucket("lucene_records").getDatabase();
	  Database oBlks = oDbEnv.openBucket("lucene_datablocks").getDatabase();	
	  oDbDir = new BDBDirectory(oTrns, oRecs, oBlks);
	} catch (Exception xcpt) {
	  throw new IOException(xcpt.getMessage(),xcpt);	
	}
	return oDbDir;
  }
  
}
