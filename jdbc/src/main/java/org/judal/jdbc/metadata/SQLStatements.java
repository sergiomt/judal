package org.judal.jdbc.metadata;

import org.judal.metadata.TypeDef;

public class SQLStatements {

	private String sqlSelect;
	private String sqlInsert;
	private String sqlUpdate;
	private String sqlDelete;
	private String sqlExists;
	private String timestampColumn;

	public String getSelect() {
		return sqlSelect;
	}

	public void setSelect(final String sqlSelect) {
		this.sqlSelect = sqlSelect;
	}

	public String getInsert() {
		return sqlInsert;
	}

	public void setInsert(final String sqlInsert) {
		this.sqlInsert = sqlInsert;
	}

	public String getUpdate() {
		return sqlUpdate;
	}

	public void setUpdate(final String sqlUpdate) {
		this.sqlUpdate = sqlUpdate;
	}

	public String getDelete() {
		return sqlDelete;
	}

	public void setDelete(final String sqlDelete) {
		this.sqlDelete = sqlDelete;
	}

	public String getExists() {
		return sqlExists;
	}

	public void setExists(final String sqlExists) {
		this.sqlExists = sqlExists;
	}

	public String getTimestampColumn() {
		return timestampColumn;
	}

	public void setTimestampColumn(final String timestampColumn) {
		this.timestampColumn = timestampColumn;
	}
}
