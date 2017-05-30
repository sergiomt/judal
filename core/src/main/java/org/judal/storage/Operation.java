package org.judal.storage;

import javax.jdo.JDOException;

import org.judal.storage.keyvalue.Stored;

public interface Operation extends AutoCloseable {

	  boolean exists(Object key);
	  
	  Stored load(Object key) throws JDOException;
	  
	  void store() throws JDOException;

	  void delete(Object key) throws JDOException;
	  
	  DataSource dataSource() throws JDOException;
	  
}
