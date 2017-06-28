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

import java.io.Serializable;

import javax.jdo.JDOException;

import org.judal.storage.DataSource;

/**
 * <p>Interface for key-value pairs.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface Stored extends Serializable {

	/**
	 * <p>Set key.</p>
	 * @param key Object
	 * @throws JDOException
	 */
	void setKey(Object key) throws JDOException;

	/**
	 * <p>Get key.</p>
	 * @return Object
	 * @throws JDOException
	 */
	Object getKey() throws JDOException;

	/**
	 * <p>Set value.</p>
	 * @param value Serializable
	 * @throws JDOException
	 */
	void setValue(Serializable value) throws JDOException;

	/**
	 * <p>Set value as a byte array.</p>
	 * @param bytes byte[]
	 * @param contentType String MIME Type
	 * @throws JDOException
	 */
	void setContent(byte[] bytes, String contentType) throws JDOException;

	/**
	 * <p>Get value.</p>
	 * @return Object
	 * @throws JDOException
	 */
	Object getValue() throws JDOException;

	/**
	 * <p>Get name of the bucket where this key-value is stored.</p>
	 * @return String
	 */
	String getBucketName();

	/**
	 * <p>Load value into <b>this</b> Stored using the default key-value data source set at EngineFactory for the thread local or the StorageContext.</p>
	 * If no value is found with given key then <b>this</b> is not modified.
	 * @param key Object
	 * @return boolean <b>true</b> if value was found and loaded, <b>false</b> otherwise.
	 * @throws JDOException
	 */
	boolean load(Object key) throws JDOException;

	/**
	 * <p>Load value into <b>this</b> Stored using the provided data source.</p>
	 * If no value is found with given key then <b>this</b> is not modified.
	 * @param dataSource DataSource
	 * @param key Object
	 * @return boolean <b>true</b> if value was found and loaded, <b>false</b> otherwise.
	 * @throws JDOException
	 */
	boolean load(DataSource dataSource, Object key) throws JDOException;

	/**
	 * <p>Store the value held by this Stored using the default key-value data source set at EngineFactory for the thread local or the StorageContext.</p>
	 * A store operation will insert the Stored if it does not exist or update it if already exists.
	 * @throws JDOException
	 */
	void store() throws JDOException;

	/**
	 * <p>Store the value held by this Stored using the provided key-value data source.</p>
	 * A store operation will insert the Stored if it does not exist or update it if already exists.
	 * @param dataSource DataSource
	 * @throws JDOException
	 */
	void store(DataSource dataSource) throws JDOException;

	/**
	 * <p>Delete the value held by this Stored using the provided key-value data source.</p>
	 * @param dataSource DataSource
	 * @throws JDOException
	 */
	void delete(DataSource dataSource) throws JDOException;
	
	/**
	 * <p>Delete the value held by this Stored using the default key-value data source set at EngineFactory for the thread local or the StorageContext.</p>
	 * @throws JDOException
	 */
	void delete() throws JDOException;
	
}
