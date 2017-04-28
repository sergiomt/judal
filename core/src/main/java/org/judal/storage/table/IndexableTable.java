package org.judal.storage.table;

import javax.jdo.JDOException;

import org.judal.metadata.IndexDef.Using;
import org.judal.storage.Param;

public interface IndexableTable extends Table, IndexableView {

	int update(Param[] values, Param[] where) throws JDOException;

	int delete(Param[] where) throws JDOException;
	
	void createIndex(String indexName, boolean unique, Using indexUsing, String... columns) throws JDOException;

	void dropIndex(String indexName) throws JDOException;
	
}
