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

import javax.jdo.Extent;
import javax.jdo.JDOException;

/**
 * <p>Interface for key-value read-only buckets.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface ReadOnlyBucket extends AutoCloseable, Extent<Stored> {

	/**
	 * <p>Bucket name.</p>
	 * @return String
	 */
	String name();

	/**
	 * <p>Check if a value with given key exists.</p>
	 * @param key Object
	 * @return boolean
	 */
	boolean exists(Object key) throws JDOException;

	/**
	 * <p>Load a key-value into a Stored instance.</p>
	 * If no value is found for given key than target is not modified
	 * @param key Object
	 * @param target Stored Cannot be null.
	 * @return boolean <b>true</b> if a value was found for the given key, <b>false</b> otherwise.
	 * @throws JDOException
	 */
	boolean load(Object key, Stored target) throws JDOException;

	/**
	 * <p>Close bucket.</p>
	 * @throws JDOException
	 */
	void close() throws JDOException;

	/**
	 * <p>Set subclass of Stored actually used by this bucket.</p>
	 * @param candidateClass Class&lt;? extends Stored&gt;
	 */
	void setClass(Class<? extends Stored> candidateClass);

	public static final int MAX_ROWS = 2147483647;
}
