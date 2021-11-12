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

import javax.jdo.JDOException;

/**
 * <p>Interface for key-value read-write buckets.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface Bucket extends ReadOnlyBucket {
	
	/**
	 * <p>Store key-value pair.</p>
	 * @param target Stored
	 * @throws JDOException
	 */
	void store(Stored target) throws JDOException;

	/**
	 * <p>Delete key-value pair given its key.</p>
	 * @param key Object
	 * @throws JDOException
	 */
	void delete(Object key) throws JDOException;

}
