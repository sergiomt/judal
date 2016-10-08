package org.judal.storage;

import javax.jdo.JDOException;

public interface RelationalDataSource extends TableDataSource {

	RelationalTable openRelationalTable(Record recordInstance) throws JDOException;
	
	RelationalView openRelationalView(Record recordInstance) throws JDOException;

}