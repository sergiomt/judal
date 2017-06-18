package org.judal.storage;

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

import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;
import javax.transaction.TransactionManager;

/**
 * Basic interface for DataSource implementations
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

	/**
	 * Get DataSource properties.
	 * Valid property names are listed at PropertyNames.
	 * @return Map&lt;String,String&gt;
	 */
	Map<String,String> getProperties();

	/**
	 * Get TransactionManager or <b>null</b> is this DataSource does not support transactions or does not use any transaction manager
	 * @return TransactionManager
	 */
	TransactionManager getTransactionManager();

	/**
	 * Get instance of JDOConnection wrapping the native connection from the DataSource implementation
	 * @return JDOConnection
	 * @throws JDOException
	 */
	JDOConnection getJdoConnection() throws JDOException;

	/**
	 * Get instance of Sequence
	 * @param name String Sequence name
	 * @return Sequence
	 * @throws JDOException
	 */
	Sequence getSequence(String name) throws JDOException;

	/**
	 * Call a native procedure from DataSource implementation
	 * @param statement String
	 * @param parameters Param...
	 * @return Object
	 * @throws JDOException
	 */
	Object call(String statement, Param... parameters) throws JDOException;
	
	/**
	 * Get whether this DataSource is in the middle of a Transaction
	 * @return boolean
	 * @throws JDOException
	 */
	boolean inTransaction() throws JDOException;
	
    /**
     * Used by Amazon S3 properties
     */
	public static final String ACCESSKEY = "accessKey";
    /**
     * Used by Amazon S3 properties
     */
    public static final String BUCKET = "bucket";
    /**
     * Used by Amazon S3 properties
     */
    public static final String REGION = "region";
    /**
     * Used by Amazon S3 properties
     */
    public static final String SECRETKEY = "secretKey";
    /**
     * Used by HBASE properties
     */
    public static final String CONFIG = "config";
    /**
     * Used by JDBC properties
     */
    public static final String DRIVER = "driver";
    /**
     * Used by Berkeley DB properties
     */
    public static final String DBENV = "dbenvironment";
    /**
     * Used by JDBC properties
     */
    public static final String URI = "uri";
    /**
     * Used by JDBC properties
     */
    public static final String CATALOG = "catalog";
    /**
     * Used by JDBC properties
     */
    public static final String USE_DATABASE_METADATA = "useDatabaseMetadata";
    /**
     * Used by JDBC properties
     */    
    public static final String SCHEMA = "schema";
    public static final String METADATA = "metadata";
    public static final String PACKAGE = "package";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String STORED = "stored";
    public static final String EXTURL = "exturl";
    public static final String LOGINTIMEOUT = "logintimeout";
    public static final String CONNECTIONTIMEOUT = "connectiontimeout";
    public static final String POOLSIZE = "poolsize";
    public static final String MAXPOOLSIZE = "maxpoolsize";
    public static final String MAXCONNECTIONS = "maxconnections";
    public static final String TRANSACTIONAL = "transactional";
    public static final String AUTOCOMMIT = "autocommit";
    
    public static String DEFAULT_POOLSIZE = "10";
    public static String DEFAULT_MAXPOOLSIZE = "100";
    public static String DEFAULT_MAXCONNECTIONS = "100";
    public static String DEFAULT_LOGINTIMEOUT = "20";
    public static String DEFAULT_CONNECTIONTIMEOUT = "60000";
    public static String DEFAULT_REGION = "eu-west-1";
    public static String DEFAULT_TRANSACTIONAL = "true";
    public static String DEFAULT_USE_DATABASE_METADATA = "true";
    public static String DEFAULT_AUTOCOMMIT = "false";
    
    /**
     * List of supported property names
     */
    public static final String[] PropertyNames = new String[]{ACCESSKEY,SECRETKEY,AUTOCOMMIT,CATALOG,CONFIG,DRIVER,DBENV,BUCKET,URI,SCHEMA,METADATA,PACKAGE,USER,PASSWORD,REGION,STORED,EXTURL,LOGINTIMEOUT,CONNECTIONTIMEOUT,POOLSIZE,MAXPOOLSIZE,MAXCONNECTIONS,TRANSACTIONAL,USE_DATABASE_METADATA};

    /**
     * List of default property values
     */
    public static final String[][] DefaultValues = new String[][]{
    	new String[]{AUTOCOMMIT,DEFAULT_AUTOCOMMIT},
    	new String[]{CONNECTIONTIMEOUT,DEFAULT_CONNECTIONTIMEOUT},
    	new String[]{LOGINTIMEOUT,DEFAULT_LOGINTIMEOUT},
    	new String[]{TRANSACTIONAL,DEFAULT_TRANSACTIONAL},
    	new String[]{MAXCONNECTIONS,DEFAULT_MAXCONNECTIONS},
    	new String[]{MAXPOOLSIZE,DEFAULT_MAXPOOLSIZE},
    	new String[]{POOLSIZE,DEFAULT_POOLSIZE},
    	new String[]{REGION,DEFAULT_REGION}
    };

    @Override
    void close() throws JDOException;
}