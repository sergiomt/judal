package org.judal.storage;

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

import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;
import javax.transaction.TransactionManager;

/**
 * 
 * @author Sergio Montoro Ten
 * @version 1.0
 */

public interface DataSource extends AutoCloseable {

	/**
	 * Checks if an object exists at the data source
	 * @param objectName String Object name
	 * @param objectType String Object type
	 *        C = CHECK constraint
	 *        D = Default or DEFAULT constraint
	 *        F = FOREIGN KEY constraint
	 *        L = Log
	 *        P = Stored procedure
	 *        PK = PRIMARY KEY constraint (type is K)
	 *        RF = Replication filter stored procedure
	 *        S = System table
	 *        TR = Trigger
	 *        U = User table
	 *        UQ = UNIQUE constraint (type is K)
	 *        V = View
	 *        X = Extended stored procedure
	 * @return <b>true</b> if object exists, <b>false</b> otherwise
	 * @throws JDOException
	 */
	boolean exists(String objectName, String objectType) throws JDOException;

	Map<String,String> getProperties();

	TransactionManager getTransactionManager();

	JDOConnection getJdoConnection() throws JDOException;

	Sequence getSequence(String name) throws JDOException;

	Object call(String statement, Param... parameters) throws JDOException;
	
	boolean inTransaction() throws JDOException;
	
    public static final String DRIVER = "driver";
    public static final String URI = "uri";
    public static final String SCHEMA = "schema";
    public static final String METADATA = "metadata";
    public static final String PACKAGE = "package";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String REGION = "region";
    public static final String LOGINTIMEOUT = "logintimeout";
    public static final String CONNECTIONTIMEOUT = "connectiontimeout";
    public static final String POOLSIZE = "poolsize";
    public static final String MAXPOOLSIZE = "maxpoolsize";
    public static final String MAXCONNECTIONS = "maxconnections";
    
    public static String DEFAULT_POOLSIZE = "10";
    public static String DEFAULT_MAXPOOLSIZE = "100";
    public static String DEFAULT_MAXCONNECTIONS = "100";
    public static String DEFAULT_LOGINTIMEOUT = "20";
    public static String DEFAULT_CONNECTIONTIMEOUT = "60000";
    public static String DEFAULT_REGION = "eu-west-1";
	
    public static final String[] PropertyNames = new String[]{DRIVER,URI,SCHEMA,METADATA,PACKAGE,USER,PASSWORD,REGION,LOGINTIMEOUT,CONNECTIONTIMEOUT,POOLSIZE,MAXPOOLSIZE,MAXCONNECTIONS};

    public static final String[][] DefaultValues = new String[][]{
    	new String[]{CONNECTIONTIMEOUT,DEFAULT_CONNECTIONTIMEOUT},
    	new String[]{LOGINTIMEOUT,DEFAULT_LOGINTIMEOUT},
    	new String[]{MAXCONNECTIONS,DEFAULT_MAXCONNECTIONS},
    	new String[]{MAXPOOLSIZE,DEFAULT_MAXPOOLSIZE},
    	new String[]{POOLSIZE,DEFAULT_POOLSIZE},
    	new String[]{REGION,DEFAULT_REGION}
    };
    
    void close() throws JDOException;
}
