package org.judal.storage.relational;

import org.judal.storage.table.Record;
import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.table.IndexableTableOperation;

public class RelationalOperation<R extends Record> extends IndexableTableOperation<R> {
	  
  public RelationalOperation(RelationalDataSource dataSource) {
	super(dataSource);
  }

  public RelationalOperation(RelationalDataSource dataSource, R record) {
  	super(dataSource, record);
  }

  protected void open() {
	  tbl = ((RelationalDataSource) dts).openRelationalTable(getRecord());
  }
  
}
