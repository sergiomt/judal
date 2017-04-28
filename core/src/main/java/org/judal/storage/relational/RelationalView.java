package org.judal.storage.relational;

import javax.jdo.JDOException;

import org.judal.storage.query.AbstractQuery;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

public interface RelationalView extends IndexableView {

	  AbstractQuery newQuery() throws JDOException;
	  
	  <R extends Record> RecordSet<R> fetch(AbstractQuery query) throws JDOException;
	
}
