package org.judal.storage.queue;

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

import java.util.Properties;

import javax.jdo.JDOException;

import org.judal.storage.Param;
import org.judal.storage.table.Record;

/**
 * <p>Record queue consumer.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface RecordQueueProducer extends AutoCloseable {
		  	
  /**
   * @throws JDOException
   */
  void close() throws JDOException;

  /**
   * <p>Send a Record to the queue for asynchronous insert or update.</p>
   * @param rec Record
   * @throws JDOException
   */
  void store(Record rec) throws JDOException;

  /**
   * <p>Send records to the queue for asynchronous insert or update.</p>
   * @param recs Record[]
   * @throws JDOException
   */
  void store(Record[] recs) throws JDOException;
  
  /**
   * <p>Send a Record to the queue for synchronous or asynchronous insert or update.</p>
   * @param rec Record
   * @param props Properties
   * @throws JDOException
   */
  void store(Record rec, Properties props) throws JDOException;

  /**
   * <p>Send records to the queue for synchronous or asynchronous insert or update.</p>
   * @param recs Record[]
   * @param props Properties
   * @throws JDOException
   */
  void store(Record[] recs, Properties props) throws JDOException;
  
  /**
   * <p>Send records to the queue for asynchronous insert.</p>
   * @param rec Record Instance of Record subclass
   * @param params Param[] Values to be inserted in the table used by Record
   * @throws JDOException
   */
  void insert(Record rec, Param[] params) throws JDOException;
  
  /**
   * <p>Send records to the queue for synchronous or asynchronous insert.</p>
   * @param rec Record Instance of Record subclass
   * @param values Param[] Values to be inserted in the table used by Record
   * @param props Properties
   * @throws JDOException
   */
  void insert(Record rec, Param[] values, Properties props) throws JDOException;
  
  /**
   * <p>Send records to the queue for asynchronous update.</p>
   * @param rec Record Instance of Record subclass
   * @param values Param[] Values to be updated in the table used by Record
   * @param where Param[] Values for the filter clause defining the records to be updated
   * @throws JDOException
   */
  void update(Record rec, Param[] values, Param[] where) throws JDOException;

  /**
   * <p>Send records to the queue for synchronous or asynchronous update.</p>
   * @param rec Record Instance of Record subclass
   * @param values Param[] Values to be updated in the table used by Record
   * @param where Param[] Values for the filter clause defining the records to be updated
   * @param props Properties
   * @throws JDOException
   */
  void update(Record rec, Param[] values, Param[] where, Properties props) throws JDOException;
  
  /**
   * <p>Send records to the queue for synchronous or asynchronous deletion.</p>
   * @param rec Record Instance of Record subclass
   * @param keys String[] Values of the primary keys of the records to be deleted
   * @param props Properties
   * @throws JDOException
   */
  void delete(Record rec, String[] keys, Properties props) throws JDOException;
  
  /**
   * <p>Wait for a certain time and call close.</p>
   * @param inmediate boolean If <b>true</b> Then ignore iTimeout and stop immediately.
   * @param timeout int Time to wait in milliseconds before stopping.
   */
  void stop(boolean inmediate, int timeout) throws JDOException;

}
