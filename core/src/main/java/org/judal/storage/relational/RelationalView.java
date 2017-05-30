package org.judal.storage.relational;

import javax.jdo.JDOException;

import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Predicate;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

public interface RelationalView extends IndexableView {

	  AbstractQuery newQuery() throws JDOException;
	  
	  Predicate newPredicate();
	  
	  <R extends Record> RecordSet<R> fetch(AbstractQuery query) throws JDOException;
	
	  Long count (Predicate filterPredicate) throws JDOException;
	  
	  Number avg (String result, Predicate filterPredicate) throws JDOException;

	  Number sum (String result, Predicate filterPredicate) throws JDOException;
	  
	  Object max (String result, Predicate filterPredicate) throws JDOException;

	  Object min (String result, Predicate filterPredicate) throws JDOException;
	  
}
