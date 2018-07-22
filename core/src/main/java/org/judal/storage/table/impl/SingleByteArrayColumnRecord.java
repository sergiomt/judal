package org.judal.storage.table.impl;

import java.sql.Types;

import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.ViewDef;

public class SingleByteArrayColumnRecord extends SingleObjectColumnRecord  {

    private static final long serialVersionUID = 1L;

    public SingleByteArrayColumnRecord(ViewDef tableDef) {
        super(tableDef);
        setColumn(new ColumnDef(columnName, Types.LONGVARBINARY, 1));
    }

    public SingleByteArrayColumnRecord(String tableName) {
        super(tableName);
        setColumn(new ColumnDef(columnName, Types.LONGVARBINARY, 1));
    }

    public SingleByteArrayColumnRecord(String tableName, String columnName) {
        super(tableName, columnName);
        setColumn(new ColumnDef(columnName, Types.LONGVARBINARY, 1));
    }

    @Override
    public boolean isEmpty(String colname) {
        return value == null;
    }

    @Override
    public Object apply(String colname) {
        return getValue();
    }

    @Override
    public Object getKey() throws JDOException {
        return getValue();
    }

    @Override
    public Object getValue() throws JDOException {
        return value;
    }

}
