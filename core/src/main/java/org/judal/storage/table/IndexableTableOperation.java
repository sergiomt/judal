package org.judal.storage.table;

import org.judal.storage.table.TableOperation;
import org.judal.storage.Param;
import org.judal.storage.table.Record;

public class IndexableTableOperation<R extends Record> extends TableOperation<R> {
	    
  public IndexableTableOperation(TableDataSource dataSource) {
	  super(dataSource);
  }
  public IndexableTableOperation(TableDataSource dataSource, R record) {
	  super(dataSource, record);
  }
  
  protected void open() {
	  tbl = ((TableDataSource) dts).openIndexedTable(getRecord());
  }
  
  @Override
  public IndexableTable getTable() {
	return (IndexableTable) tbl;  
  }
  
  public int update(Param[] values, Param[] where) {
	  return getTable().update(values, where);
  }
}
