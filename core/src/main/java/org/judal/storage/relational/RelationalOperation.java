package org.judal.storage.relational;

import org.judal.storage.EngineFactory;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.table.Record;
import org.judal.storage.relational.RelationalDataSource;
import org.judal.storage.table.IndexableTableOperation;

public class RelationalOperation<R extends Record> extends IndexableTableOperation<R> {

	public RelationalOperation() {
		this((RelationalDataSource) EngineFactory.DefaultThreadDataSource.get());
	}

	public RelationalOperation(R record) {
		 this((RelationalDataSource) EngineFactory.DefaultThreadDataSource.get(), record);
	 }

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
