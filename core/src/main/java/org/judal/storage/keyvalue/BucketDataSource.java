package org.judal.storage.keyvalue;

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
import javax.jdo.JDOUserException;

import org.judal.storage.DataSource;
import org.judal.storage.EngineFactory;
import org.judal.storage.StorageContext;

public interface BucketDataSource extends DataSource {

	void createBucket(String bucketName, Map<String,Object> options) throws JDOException;

	Bucket openBucket(String bucketName) throws JDOException;

	void dropBucket(String bucketName) throws JDOException;

	void truncateBucket(String bucketName) throws JDOException;
	
}
