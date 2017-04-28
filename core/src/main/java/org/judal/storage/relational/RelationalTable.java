package org.judal.storage.relational;

import javax.jdo.JDOException;

import org.judal.storage.Param;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.table.IndexableTable;

public interface RelationalTable extends IndexableTable,RelationalView {

	int update(Param[] values, AbstractQuery filter) throws JDOException;

	int delete(AbstractQuery filter) throws JDOException;

}
