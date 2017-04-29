package org.judal.jdbc;

import java.sql.SQLException;
import java.util.Map;

import javax.jdo.JDOException;
import javax.transaction.TransactionManager;

import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.relational.RelationalTable;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.Record;

public class JDBCRelationalDataSource extends JDBCTableDataSource implements RelationalDataSource {

  public JDBCRelationalDataSource(Map<String, String> properties, TransactionManager transactManager)
	throws SQLException, NumberFormatException, ClassNotFoundException, NullPointerException, UnsatisfiedLinkError {
	  super(properties,transactManager);
  }

	@Override
	public JDBCRelationalTable openRelationalTable(Record tableRecord) throws JDOException {
		return openTable(tableRecord);
	}
	
	@Override
	public JDBCRelationalView openRelationalView(Record viewRecord) throws JDOException {
		return new JDBCRelationalView(this, viewRecord);
	}

	@Override
	public int getRdbmsId() {
		return databaseProductId.intValue();
	}

}
