package org.judal.storage.table.comparators;

import java.sql.Types;

public class RecordColumnDoubleComparatorAsc extends RecordColumnValueComparatorAsc {

    public RecordColumnDoubleComparatorAsc(String sColumnName) {
        super(sColumnName, Types.DOUBLE);
      }
	
}
