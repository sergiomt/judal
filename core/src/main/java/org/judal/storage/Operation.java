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

import javax.jdo.JDOException;

import org.judal.storage.keyvalue.Stored;

/**
 * <p>Interface for load, store, exists and delete operation wrapper.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface Operation extends AutoCloseable {

	  /**
	   * <p>Check whether a key-value or record exists with the given primary key.</p>
	   * @param key Object 
	   * @return boolean
	   */
	  boolean exists(Object key);
	  
	  /**
	   * <p>Load value or record given its key.</p>
	   * @param key Object
	   * @return Stored
	   * @throws JDOException
	   */
	  Stored load(Object key) throws JDOException;
	  
	  /**
	   * <p>Store the key-value or record associated with this operation.</p>
	   * @throws JDOException
	   */
	  void store() throws JDOException;

	  /**
	   * <p>Delete value or record given its key.</p>
	   * @param key Object
	   * @throws JDOException
	   */
	  void delete(Object key) throws JDOException;
	  
	  /**
	   * <p>Get the data source over which this operation runs.</p>
	   * @return DataSource
	   * @throws JDOException
	   */
	  DataSource dataSource() throws JDOException;
	  
}
