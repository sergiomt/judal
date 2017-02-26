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

import java.io.Serializable;

import javax.jdo.JDOException;

/**
 * 
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface Stored extends Serializable {

	void setKey(Object key) throws JDOException;

	Object getKey() throws JDOException;

	void setValue(Serializable value) throws JDOException;

	void setContent(byte[] bytes, String contentType) throws JDOException;

	Object getValue() throws JDOException;

	String getBucketName();

	boolean load(DataSource dataSource, Object key) throws JDOException;

	void store(DataSource dataSource) throws JDOException;

	void delete(DataSource dataSource) throws JDOException;
	
	
}
