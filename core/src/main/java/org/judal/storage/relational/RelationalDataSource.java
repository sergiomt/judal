package org.judal.storage.relational;

import java.util.Map.Entry;

import javax.jdo.JDOException;

import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;

public interface RelationalDataSource extends TableDataSource {

	RelationalTable openRelationalTable(Record recordInstance) throws JDOException;
	
	RelationalView openRelationalView(Record recordInstance) throws JDOException;
	
	RelationalView openInnerJoinView(Record recordInstance1, String joinedTableName, Entry<String,String> column) throws JDOException;

	RelationalView openOuterJoinView(Record recordInstance1, String joinedTableName, Entry<String,String> column) throws JDOException;

	RelationalView openInnerJoinView(Record recordInstance1, String joinedTableName, Entry<String,String>[] columns) throws JDOException;

	RelationalView openOuterJoinView(Record recordInstance1, String joinedTableName, Entry<String,String>[] columns) throws JDOException;

	int getRdbmsId();

}