package org.judal.storage.keyvalue;

/*
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

import org.judal.storage.DataSource;

/**
 * <p>Interface for data sources providing key-value pairs</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface BucketDataSource extends DataSource {

	/**
	 * <p>Create a container for key-value pairs.</p>
	 * @param bucketName String Bucket name
	 * @param options Map&lt;String,Object&gt; with entries defined by the implementation
	 * @throws JDOException
	 */
	void createBucket(String bucketName, Map<String,Object> options) throws JDOException;

	/**
	 * <p>Get instance of an existing Bucket.</p>
	 * @param bucketName String
	 * @return Bucket
	 * @throws JDOException
	 */
	Bucket openBucket(String bucketName) throws JDOException;

	/**
	 * <p>Drop an existing Bucket.</p>
	 * @param bucketName String
	 * @throws JDOException
	 */
	void dropBucket(String bucketName) throws JDOException;

	/**
	 * <p>Remove all key-value pairs contained in a Bucket.</p>
	 * @param bucketName String
	 * @throws JDOException
	 */
	void truncateBucket(String bucketName) throws JDOException;
	
}
