package org.judal.storage.table.comparators;

import org.judal.storage.table.Record;

public class RecordColumnValueComparatorDesc extends RecordColumnValueComparatorAsc {

    public RecordColumnValueComparatorDesc(String sColumnName) {
      super(sColumnName);
    }

    public RecordColumnValueComparatorDesc(String sColumnName, int columnType) {
        super(sColumnName);
      }

    public int compare(Record r1, Record r2) {
      return super.compare(r2,r1);
    }    
}