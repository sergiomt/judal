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

public interface RecordQueueProducer extends AutoCloseable {
		  	
  void close() throws JDOException;

  void store(Record rec) throws JDOException;

  void store(Record[] recs) throws JDOException;
  
  void store(Record rec, Properties props) throws JDOException;

  void store(Record[] recs,Properties props) throws JDOException;
  
  void insert(Record rec, Param[] params) throws JDOException;
  
  void insert(Record oRec, Param[] values, Properties props) throws JDOException;
  
  void update(Record rec, Param[] values, Param[] where) throws JDOException;

  void update(Record rec, Param[] values, Param[] where, Properties props) throws JDOException;
  
  void delete(Record rec, String[] keys, Properties props) throws JDOException;
  
  void stop(boolean inmediate, int timeout) throws JDOException;

}
