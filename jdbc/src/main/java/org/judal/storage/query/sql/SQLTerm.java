package org.judal.storage.query.sql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;

import org.judal.storage.query.Operator;
import org.judal.storage.query.Part;
import org.judal.storage.query.Predicate;
import org.judal.storage.query.Term;


/*
* Represents a fragment of the WHERE clause of a SQL statement
* @author Sergio Montoro Ten
* @version 1.0
*/
public class SQLTerm extends Term {

	private static final long serialVersionUID = 1L;

	public SQLTerm(String columnName, String operator, Object columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, String columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, Boolean columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}
	
	public SQLTerm(String columnName, String operator, Integer columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, Long columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, Float columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, Double columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, BigDecimal columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, BigInteger columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, boolean columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, short columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, int columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, long columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, float columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, double columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}
	
	public SQLTerm(String columnName, String operator, String tableName, Part nestedTerm) {
		super (columnName, operator, tableName, nestedTerm);
	}

	public SQLTerm(String columnName, String operator, String tableName, String nestedColumnName, Part nestedTerm) {
		super (columnName, operator, tableName, nestedColumnName, nestedTerm);
	}
	
	public SQLTerm(String columnName, String operator, String[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Integer[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, int[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Long[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, long[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Float[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, float[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Double[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, double[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Date[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Timestamp[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, BigDecimal[] columnValues) {
		super (columnName, operator, columnValues);
	}
	
	public SQLTerm(String columnName, String operator, double d1, double d2, double d3) {
		super (columnName, operator, d1, d2, d3);
	}

	public SQLTerm(String columnName, String operator, String tableName, String joinColumn, SQLTerm joinTerm) {
		super(columnName, operator, tableName,  joinColumn, joinTerm);
	}
	
	/**
	 * Get the term as a String including all parameter values
	 * @return String
	 */
	@Override
	public String getText() {
		String sTxt;
		final Object value0 = aValues[0];
		if (value0 instanceof Term || value0 instanceof Predicate) {
			if (sNestedColumn==null)
				throw new NullPointerException("Column name for subquery cannot be null");
			Part oNested = (Part) value0;			
			return sColumn+" "+sOper +" (SELECT " + sNestedColumn +" FROM "+ getTableName() +" WHERE "+oNested.getText()+ ")";
		} else {
			if (sOper.equalsIgnoreCase(Operator.WITHIN)) {
				sTxt = Operator.WITHIN+"("+sColumn+", ST_GeographyFromText('SRID=4326;POINT("+aValues[0].toString()+" "+aValues[1].toString()+")'),"+aValues[2].toString()+")";
			} else if (sOper.equalsIgnoreCase(Operator.BETWEEN)) {
				sTxt = sColumn+" "+Operator.BETWEEN+" "+value0.toString()+" AND "+value0.toString()+" ";
			} else if (sOper.equalsIgnoreCase(Operator.EXISTS) || sOper.equalsIgnoreCase(Operator.NOTEXISTS)) {
				sTxt = sOper+" ("+replaceParamaters(sColumn)+")";
			} else if (sOper.equalsIgnoreCase(Operator.IS) || sOper.equalsIgnoreCase(Operator.ISNOT)) {
				sTxt = sColumn+" "+sOper+" "+(value0==null ? "NULL" : value0);
			} else if (sOper.equalsIgnoreCase(Operator.IN) || sOper.equalsIgnoreCase(Operator.NOTIN)) {
				if (aValues[0] instanceof String) {
					sTxt = sColumn+" "+sOper+" ('"+value0.toString()+"'";
					for (int v=1; v<nValues; v++)
						sTxt += ",'"+aValues[v].toString()+"'";
					sTxt += ")";				
				} else {
					sTxt = sColumn+" "+sOper+" ('"+value0.toString();
					for (int v=1; v<nValues; v++)
						sTxt += ","+aValues[v].toString();
					sTxt += ")";				
				}
			} else {
				sTxt = sColumn+" "+sOper+" ";
				if (aValues[0] instanceof String)
					sTxt += "'" + aValues[0] + "'";
				else
					if (aValues[0]==null)
						sTxt += "null";			
					else
						sTxt += aValues[0].toString();			
			}
			return sTxt;			
		}
	}
	
	/**
	 * Get the term as a String with parameters as SQL question marks
	 * @return String
	 */
	@Override	
	public String getTextParametrized() {
		String sTxt;
		final Object value0 = aValues[0];
		if (value0 instanceof Term || value0 instanceof Predicate) {
			if (sNestedColumn==null)
				throw new NullPointerException("Column name for subquery cannot be null");
			Part oNested = (Part) value0;
			return sColumn+" "+sOper +" (SELECT " + sNestedColumn +" FROM "+getTableName()+" WHERE "+oNested.getTextParametrized()+ ")";
		} else {
			if (sOper.equalsIgnoreCase(Operator.WITHIN)) {
				sTxt = Operator.WITHIN+"("+sColumn+", ST_SetSRID(ST_MakePoint(?,?),4326),?)";
			} else if (sOper.equalsIgnoreCase(Operator.BETWEEN)) {
				sTxt = sColumn+" "+Operator.BETWEEN+" ? AND ? ";
			} else if (sOper.equalsIgnoreCase(Operator.EXISTS) || sOper.equalsIgnoreCase(Operator.NOTEXISTS)) {
				sTxt = sOper+" ("+sColumn+")";
			} else if (sOper.equalsIgnoreCase(Operator.IS) || sOper.equalsIgnoreCase(Operator.ISNOT)) {
				sTxt = sColumn+" "+sOper+" "+(aValues[0]==null ? "NULL" : value0);
			} else if (sOper.equalsIgnoreCase(Operator.IN) || sOper.equalsIgnoreCase(Operator.NOTIN)) {
				sTxt = sColumn+" "+sOper+" (?";
				for (int v=1; v<nValues; v++)
					sTxt += ",?";
				sTxt += ")";
			} else {
				sTxt = sColumn+" "+sOper+" ?";
			}
			return sTxt;			
		}
	}
	
}
