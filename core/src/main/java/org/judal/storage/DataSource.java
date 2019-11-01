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
 * Base interface for DataSource implementations
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
     * Configuration property used by Amazon S3
     */
	public static final String ACCESSKEY = "accessKey";
    /**
     * Lucene Analyser class. Default is org.apache.lucene.analysis.WhitespaceAnalyzer.
     */
    public static final String ANALYZER = "analyzer";
	/**
     * Configuration property used by JDBC. Boolean (true/false). Default true.
     */
    public static final String AUTOCOMMIT = "autocommit";
    /**
     * Configuration property used by Amazon S3
     */
    public static final String BUCKET = "bucket";
    /**
     * Configuration property used by Amazon S3
     */
    public static final String REGION = "region";
    /**
     * Configuration property used by Amazon S3 and Google Firebase
     */
    public static final String SECRETKEY = "secretKey";
    /**
     * Configuration property used by HBASE. String. Path to HBASE XML configuration file.
     */
    public static final String CONFIG = "config";
    /**
     * Configuration property used by JDBC. String. Fully qualified name of JDBC driver class.
     */
    public static final String DRIVER = "driver";
    /**
     * Configuration property used by Berkeley DB. String. Path to Berkeley DB base directory.
     */
    public static final String DBENV = "dbenvironment";
    /**
     * Configuration property used by JDBC. Database connection string.
     */
    public static final String URI = "uri";
    /**
     * Configuration property used by JDBC. Catalog name.
     */
    public static final String CATALOG = "catalog";
    /**
     * Configuration property used by JDBC. Boolean (true/false). Default true.
     */
    public static final String USE_DATABASE_METADATA = "useDatabaseMetadata";
    /**
     * Configuration property used by JDBC. Schema name.
     */
    public static final String SCHEMA = "schema";
    /**
     * Configuration property. Name of file describing schema metadata. Default "metadata.xml"
     */
    public static final String METADATA = "metadata";
    /**
     * Configuration property. Path of package containing metadata like "com/acme/app/model"
     */
    public static final String PACKAGE = "package";
    /**
     * Configuration property. Username.
     */
    public static final String USER = "user";
    /**
     * Configuration property. Clear Password.
     */
    public static final String PASSWORD = "password";
    /**
     * Configuration property used by Google Firebase.
     */
    public static final String PROJECTID = "projectid";
    /**
     * Configuration property
     */
    public static final String STORED = "stored";
    /**
     * Configuration property
     */
    public static final String EXTURL = "exturl";
    /**
     * Configuration property. int [-1..n] Seconds.
     */
    public static final String LOGINTIMEOUT = "logintimeout";
    /**
     * Configuration property. int [-1..n] Seconds.
     */
    public static final String CONNECTIONTIMEOUT = "connectiontimeout";
    /**
     * Configuration property. int [0..n] Size of JDBC connection pool.
     */
    public static final String POOLSIZE = "poolsize";
    /**
     * Configuration property. int [POOLSIZE..n] Maximum size of JDBC connection pool.
     */
    public static final String MAXPOOLSIZE = "maxpoolsize";
    /**
     * Configuration property. int [MAXPOOLSIZE..n] Maximum number of allowed JDBC connections.
     */
    public static final String MAXCONNECTIONS = "maxconnections";
    /**
     * Configuration property. Boolean (true/false) Default true
     */
    public static final String TRANSACTIONAL = "transactional";
    /**
     * LDAP Connection string. For example ldap://192.168.101.110:389/dc=auth,dc=com 
     */
    public static final String LDAPCONNECT = "ldapconnect";
    /**
     * LDAP Admin user.  For example cn=Manager,dc=auth,dc=com
     */
    public static final String LDAPUSER = "ldapuser";
    /**
     * LDAP Admin user pasword
     */
    public static final String LDAPPASSWORD = "ldappassword";
    /**
     * Lucene index directory
     */
    public static final String LUCENEINDEX = "luceneindex";
    /**
     * Hash algorithm
     */
    public static final String HASHALGORITHM = "hashalgorithm";
    /**
     * SHA salt
     */
    public static final String SALT = "salt";

    public static final String MAIL_STORE_PROTOCOL = "mail.store.protocol";
    public static final String MAIL_TRANSPORT_PROTOCOL = "mail.transport.protocol";
    public static final String MAIL_INCOMING = "mail.incoming";
    public static final String MAIL_OUTGOING = "mail.outgoing";
    public static final String MAIL_ACCOUNT = "mail.account";
    public static final String MAIL_PASSWORD = "mail.password";
    public static final String MAIL_USER = "mail.user";
    public static final String MAIL_SMTP_HOST = "mail.smtp.host";
    public static final String MAIL_SMTP_PORT = "mail.smtp.port";

    public static String DEFAULT_POOLSIZE = "10";
    public static String DEFAULT_MAXPOOLSIZE = "100";
    public static String DEFAULT_MAXCONNECTIONS = "100";
    public static String DEFAULT_LOGINTIMEOUT = "20";
    public static String DEFAULT_CONNECTIONTIMEOUT = "60000";
    public static String DEFAULT_REGION = "eu-west-1";
    public static String DEFAULT_TRANSACTIONAL = "true";
    public static String DEFAULT_USE_DATABASE_METADATA = "true";
    public static String DEFAULT_AUTOCOMMIT = "false";
    public static String DEFAULT_METADATA = "metadata.xml";

    public static final String DEFAULT_MAIL_STORE_PROTOCOL = "pop3";
    public static final String DEFAULT_MAIL_TRANSPORT_PROTOCOL = "smtp";
    public static final String DEFAULT_MAIL_SMTP_PORT = "25";

    public static final String DEFAULT_ANALYZER = "org.apache.lucene.analysis.WhitespaceAnalyzer";

    /**
     * List of supported property names
     */
    public static final String[] PropertyNames = new String[]{ACCESSKEY,ANALYZER,SECRETKEY,AUTOCOMMIT,CATALOG,CONFIG,DRIVER,DBENV,BUCKET,URI,SCHEMA,METADATA,PACKAGE,USER,PASSWORD,PROJECTID,REGION,HASHALGORITHM,SALT,STORED,EXTURL,LOGINTIMEOUT,CONNECTIONTIMEOUT,POOLSIZE,MAXPOOLSIZE,MAXCONNECTIONS,TRANSACTIONAL,USE_DATABASE_METADATA,LDAPCONNECT,LDAPUSER,LDAPPASSWORD,MAIL_STORE_PROTOCOL, MAIL_TRANSPORT_PROTOCOL, MAIL_INCOMING, MAIL_OUTGOING, MAIL_ACCOUNT, MAIL_PASSWORD, MAIL_USER, MAIL_SMTP_HOST, MAIL_SMTP_PORT,LUCENEINDEX};

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
    	new String[]{REGION,DEFAULT_REGION},
    	new String[]{ANALYZER,DEFAULT_ANALYZER}
    };

    @Override
    void close() throws JDOException;

}