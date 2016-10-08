package org.judal.storage;

import javax.jdo.JDOException;

import org.judal.storage.query.AbstractQuery;

public interface RelationalView extends IndexableView {

	  AbstractQuery newQuery() throws JDOException;
	  
	  <R extends Record> RecordSet<R> fetch(AbstractQuery query) throws JDOException;
	
}
