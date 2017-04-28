package org.judal.storage.table;


import java.sql.Types;
import java.util.Comparator;
import java.util.Date;

public class RecordColumnValueComparatorAsc implements Comparator<Record> {

  private String sColName;
  private int iColType;
  private String dtNull;
  
  public RecordColumnValueComparatorAsc(String sColumnName) {
    sColName = sColumnName;
    iColType = Types.NULL;
    dtNull = null;
  }

  public int compare(Record r1, Record r2) {
	  if (iColType==Types.NULL) {
	  	iColType = r1.getColumn(sColName).getType();
	    if (iColType==Types.DATE || iColType==Types.TIMESTAMP)
	      dtNull = "0000-01-01 00:00:00"; 
	  }
	  switch (iColType) {

	    case Types.INTEGER:
	  	  if (r1.isNull(sColName) && r2.isNull(sColName))
	  	  	return 0;
	  	  else if (r1.isNull(sColName))
	  	  	return -1;
	  	  else if (r2.isNull(sColName))
	  	  	return 1;
	  	  else
	  	  	return r1.getInt(sColName)>r2.getInt(sColName) ? 1 : r1.getInt(sColName)<r2.getInt(sColName) ? -1 : 0;	  		

	    case Types.BIGINT:
		  if (r1.isNull(sColName) && r2.isNull(sColName))
		  	return 0;
		  else if (r1.isNull(sColName))
		  	return -1;
		  else if (r2.isNull(sColName))
		  	return 1;
		  else
		  	return r1.getLong(sColName)>r2.getLong(sColName) ? 1 : r1.getLong(sColName)<r2.getLong(sColName) ? -1 : 0;

	  	case Types.CLOB:
	    case Types.CHAR:
	    case Types.NCHAR:
	    case Types.VARCHAR:
	    case Types.NVARCHAR:
	    case Types.LONGVARCHAR:
	    case Types.LONGNVARCHAR:	    
	      return r1.getString(sColName,"").compareTo(r2.getString(sColName,""));

	    case Types.DATE:
	  	case Types.TIMESTAMP:
	  	  Date dt1, dt2;
	  	  if (r1.isNull(sColName)) dt1 = null; else dt1 = r1.getDate(sColName);
	  	  if (r2.isNull(sColName)) dt2 = null; else dt2 = r2.getDate(sColName);
	  	  if (dt1==null && dt2==null)
	  	    return 0;
	  	  else if (dt1==null)
		  	return 1;	  		
	  	  else if (dt2==null)
		  	return -1;
	  	  else
	  	    return dt1.compareTo(dt2);

		default:
		  throw new IllegalArgumentException("Column comparator not implemented for type");
	  }
  } // compare

  public boolean equals(Record r1, Record r2) {
	  if (iColType==Types.NULL) {
	  	iColType = r1.getColumn(sColName).getType();
	    if (iColType==Types.DATE || iColType==Types.TIMESTAMP)
	      dtNull = "0000-01-01 00:00:00";
	  }
	  switch (iColType) {
	  	case Types.INTEGER:
	  	  if (r1.isNull(sColName) || r2.isNull(sColName))
	  	    return false;
	  	  else
	  	  	return r1.getInt(sColName)==r2.getInt(sColName);
	  	case Types.CLOB:
	    case Types.CHAR:
	    case Types.NCHAR:
	    case Types.VARCHAR:
	    case Types.NVARCHAR:
	    case Types.LONGVARCHAR:
	    case Types.LONGNVARCHAR:
	      return r1.getString(sColName,"null").equals(r2.getString(sColName,"null"));
	  	case Types.DATE:
	  	case Types.TIMESTAMP:
	      return r1.getString(sColName,dtNull).equals(r2.getString(sColName,dtNull));
		default:
		  throw new IllegalArgumentException("Column comparator not implemented for type");
	  }
  } // equals
}
