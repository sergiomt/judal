package org.judal.storage.table.comparators;

import java.sql.Types;

public class RecordColumnDoubleComparatorDesc extends RecordColumnValueComparatorDesc {

    public RecordColumnDoubleComparatorDesc(String sColumnName) {
        super(sColumnName, Types.DOUBLE);
      }

}
