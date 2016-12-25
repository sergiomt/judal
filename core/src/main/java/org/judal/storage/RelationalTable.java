package org.judal.storage;

import javax.jdo.JDOException;

import org.judal.storage.query.AbstractQuery;

public interface RelationalTable extends IndexableTable,RelationalView {

	int update(Param[] values, AbstractQuery filter) throws JDOException;

	int delete(AbstractQuery filter) throws JDOException;

}
