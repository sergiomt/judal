package org.judal.storage.query;

import java.util.Date;

import java.io.Serializable;
import java.math.BigDecimal;

import java.sql.Time;
import java.sql.Timestamp;

/*
* @author Sergio Montoro Ten
* @version 1.0
*/
public abstract class Term implements Part,Serializable {

	private static final long serialVersionUID = 10000L;
	
	protected String sTable;
	protected String sColumn;
	protected String sNestedColumn;
	protected String sOper;
	protected Object[] aValues;
	protected int nValues;
	
	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be one of {"=","<>",">=","<=","IS","IS NOT","LIKE","ILIKE","BETWEEN"}
	 * @param oColumnValue Object column value
	 * @throws ArrayIndexOutOfBoundsException if oColumnValue is an array
	 */
	public Term(String sColumnName, String sOperator, Object oColumnValue) throws ArrayIndexOutOfBoundsException {

		sTable = null;
		sColumn = sColumnName;
		sNestedColumn = null;
		sOper = sOperator;
		if (Operator.BETWEEN.equalsIgnoreCase(sOperator)) {
			if (oColumnValue==null)
				throw new ArrayIndexOutOfBoundsException("Operator "+Operator.BETWEEN+" requires two parameters but got null");
			else if (!oColumnValue.getClass().isArray())
				throw new ArrayIndexOutOfBoundsException("Operator "+Operator.BETWEEN+" requires two parameters");
			else
				aValues = castObjectToArray(oColumnValue);
			nValues = aValues.length;
			if (nValues!=2)
				throw new ArrayIndexOutOfBoundsException("Operator "+Operator.BETWEEN+" requires two parameters but got "+String.valueOf(aValues.length));
		} else {
			if (Operator.IN.equalsIgnoreCase(sOperator) || Operator.NOTIN.equalsIgnoreCase(sOperator)) {
				if (oColumnValue==null)
					aValues = new Object[0];
				else if (!oColumnValue.getClass().isArray() && !(oColumnValue instanceof Term))
					throw new ArrayIndexOutOfBoundsException("Operator "+Operator.IN+" requires array or subselect parameter");
				else if (oColumnValue instanceof Term)
					aValues = new Object[]{oColumnValue};
				else
					aValues = castObjectToArray(oColumnValue);
				nValues = aValues.length;
			} else {
				if (null==oColumnValue)
					  aValues = new Object[]{oColumnValue};
					else if (!oColumnValue.getClass().isArray())
					  aValues = new Object[]{oColumnValue};
					else
						throw new ArrayIndexOutOfBoundsException("Array value for column "+sColumnName+" not allowed");			
				nValues = 1;				
			}
		}
	}
	
	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be one of {"IN","NOT IN","EXISTS","NOT EXISTS","LIKE","ILIKE","BETWEEN"}
	 * @param sTableName String Table Name
	 * @param oNestedTerm QueryTerm Nested Query term
	 */
	public Term(String sColumnName, String sOperator, String sTableName, Part oNestedTerm) {
		sTable = sTableName;
		sColumn = sColumnName;
		sNestedColumn = sColumnName;
		sOper = sOperator;
		aValues = new Object[]{oNestedTerm};
		nValues = 1;
	}

	/**
	 * Create term.
	 * @param sColumnName String Name of column at outer table
	 * @param sOperator String Operator. Must be one of {"IN","NOT IN","EXISTS","NOT EXISTS","LIKE","ILIKE","BETWEEN"}
	 * @param sTableName String Table Name
	 * @param sNestedColumnName String Name of column at nested table
	 * @param oNestedTerm QueryTerm Nested Query term
	 */
	public Term(String sColumnName, String sOperator, String sTableName, String sNestedColumnName, Part oNestedTerm) {
		sTable = sTableName;
		sColumn = sColumnName;
		sNestedColumn = sNestedColumnName;
		sOper = sOperator;
		aValues = new Object[]{oNestedTerm};
		nValues = 1;
	}
	
	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Must be Operator.BETWEEN or Operator.IN 
	 * @param oColumnValue String[] column values
	 */
	public Term(String sColumnName, String sOperator, String[] aColumnValues) {
		sTable = null;
	  sColumn = sColumnName;
	  sOper = sOperator;
	  nValues = aColumnValues.length;
	  aValues = new Object[nValues];
	  for (int v=0; v<nValues; v++)
		aValues[v] = aColumnValues[v];
	}

	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN 
	 * @param oColumnValue Integer[] column values
	 */
	public Term(String sColumnName, String sOperator, Integer[] aColumnValues) {
		sTable = null;
	  sColumn = sColumnName;
	  sOper = sOperator;
	  nValues = aColumnValues.length;
	  aValues = new Object[nValues];
	  for (int v=0; v<nValues; v++)
		aValues[v] = aColumnValues[v];
	}

	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN 
	 * @param oColumnValue int[] column values
	 */
	public Term(String sColumnName, String sOperator, int[] aColumnValues) {
		sTable = null;
	  sColumn = sColumnName;
	  sOper = sOperator;
	  nValues = aColumnValues.length;
	  aValues = new Object[nValues];
	  for (int v=0; v<nValues; v++)
		aValues[v] = aColumnValues[v];
	}
	
	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN 
	 * @param oColumnValue Long[] column values
	 */
	public Term(String sColumnName, String sOperator, Long[] aColumnValues) {
		sTable = null;
	  sColumn = sColumnName;
	  sOper = sOperator;
	  nValues = aColumnValues.length;
	  aValues = new Object[nValues];
	  for (int v=0; v<nValues; v++)
	    aValues[v] = aColumnValues[v];
	}

	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN 
	 * @param oColumnValue long[] column values
	 */
	public Term(String sColumnName, String sOperator, long[] aColumnValues) {
		sTable = null;
	  sColumn = sColumnName;
	  sOper = sOperator;
	  nValues = aColumnValues.length;
	  aValues = new Object[nValues];
	  for (int v=0; v<nValues; v++)
	    aValues[v] = aColumnValues[v];
	}
	
	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN 
	 * @param oColumnValue Float[] column values
	 */
	public Term(String sColumnName, String sOperator, Float[] aColumnValues) {
		sTable = null;
		sColumn = sColumnName;
		sOper = sOperator;
		nValues = aColumnValues.length;
		aValues = new Object[nValues];
		for (int v=0; v<nValues; v++)
		  aValues[v] = aColumnValues[v];
	}

	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN 
	 * @param oColumnValue Float[] column values
	 */
	public Term(String sColumnName, String sOperator, float[] aColumnValues) {
		sTable = null;
		sColumn = sColumnName;
		sOper = sOperator;
		nValues = aColumnValues.length;
		aValues = new Object[nValues];
		for (int v=0; v<nValues; v++)
		  aValues[v] = aColumnValues[v];
	}
	
	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN or Operator.WITHIN
	 * @param oColumnValue Double[] column values
	 */
	public Term(String sColumnName, String sOperator, Double[] aColumnValues) {
		sTable = null;
		sColumn = sColumnName;
		sOper = sOperator;
		nValues = aColumnValues.length;
		aValues = new Object[nValues];
		for (int v=0; v<nValues; v++)
		  aValues[v] = aColumnValues[v];
	}

	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN or Operator.WITHIN
	 * @param oColumnValue double[] column values
	 */
	public Term(String sColumnName, String sOperator, double[] aColumnValues) {
		sTable = null;
		sColumn = sColumnName;
		sOper = sOperator;
		nValues = aColumnValues.length;
		aValues = new Object[nValues];
		for (int v=0; v<nValues; v++)
		  aValues[v] = aColumnValues[v];
	}
	
	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.IN or Operator.WITHIN
	 * @param double value 1
	 * @param double value 2
	 * @param double value 3
	 */
	public Term(String sColumnName, String sOperator, double d1, double d2, double d3) {
		sTable = null;
		sColumn = sColumnName;
		sOper = sOperator;
		nValues = 3;
		aValues = new Object[nValues];
		aValues[0] = d1;
		aValues[1] = d2;
		aValues[2] = d3;
	}
	
	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN or Operator.WITHIN
	 * @param oColumnValue Date[] column values
	 */
	public Term(String sColumnName, String sOperator, Date[] aColumnValues) {
		sTable = null;
		sColumn = sColumnName;
		sOper = sOperator;
		nValues = aColumnValues.length;
		aValues = new Object[nValues];
		for (int v=0; v<nValues; v++)
		  aValues[v] = aColumnValues[v];
	}

	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN
	 * @param oColumnValue Timestamp[] column values
	 */
	public Term(String sColumnName, String sOperator, Timestamp[] aColumnValues) {
		sTable = null;
		sColumn = sColumnName;
		sOper = sOperator;
		nValues = aColumnValues.length;
		aValues = new Object[nValues];
		for (int v=0; v<nValues; v++)
		  aValues[v] = aColumnValues[v];
	}

	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN
	 * @param oColumnValue BigDecimal[] column values
	 */
	public Term(String sColumnName, String sOperator, BigDecimal[] aColumnValues) {
		sTable = null;
		sColumn = sColumnName;
		sOper = sOperator;
		nValues = aColumnValues.length;
		aValues = new Object[nValues];
		for (int v=0; v<nValues; v++)
		  aValues[v] = aColumnValues[v];
	}
	
	public String getTableName() {
		return sTable;
	}
	
	public String getColumnName() {
		return sColumn;
	}

	/**
	 * @return One of {"=","<>",">=","<=","IN","BETWEEN","IS","IS NOT","LIKE","ILIKE"}
	 */
	public String getOperator() {
		return sOper;
	}

	/**
	 * Get first parameter value
	 * @return Object
	 */
	public Object getValue() {
		return aValues[0];
	}

	/**
	 * Get a parameter value
	 * @return Object
	 */
	public Object getValue(int n) {
		return aValues[n];
	}
	
	/**
	 * Get count of parameter values
	 * @return int
	 */
	public int getValueCount() {
		return nValues;
	}

	@Override
	public int size() {
		return 1;
	}
	
	protected String replaceParamaters(String sExpr) {
		if (sExpr==null) return null;
		if (sExpr.length()==0) return "";
	  StringBuffer oRpl = new StringBuffer();
	  final int nChars = sExpr.length();
	  int v = 0;
	  for (int n=0; n<nChars; n++) {
	  	char c = sExpr.charAt(n);
	  	if (c=='?') {
	  		if (aValues[v]==null)
	  			oRpl.append("NULL");
	  		else if (aValues[v] instanceof String)
	  			oRpl.append("'"+aValues[v]+"'");
	  		else
	  			oRpl.append(aValues[v].toString());
	  	} else {
	  		oRpl.append(c);
	  	}
	  }
	  return oRpl.toString();
	}
	
	/**
	 * Get parameter values
	 * @return Object[]
	 */
	@Override
	public Object[] getParameters() {
		final Object value0 = aValues[0];
		if (value0 instanceof Term || value0 instanceof Predicate)
			return ((Part) value0).getParameters();
		else
			return aValues;
	}
	
	private Object[] castObjectToArray(Object values) {
		Class<?> objClass = values.getClass();
		Object[] objValues;
		if (objClass.equals(short[].class)) {
			short[] shrtValues = (short[]) values;
			objValues = new Object[shrtValues.length];
			for (int v=0; v<shrtValues.length; v++)
				objValues[v] = new Short(shrtValues[v]);
		}
		else if (objClass.equals(int[].class)) {
			int[] intValues = (int[]) values;
			objValues = new Object[intValues.length];
			for (int v=0; v<intValues.length; v++)
				objValues[v] = new Integer(intValues[v]);
		}
		else if (objClass.equals(long[].class)) {
			long[] lngValues = (long[]) values;
			objValues = new Object[lngValues.length];
			for (int v=0; v<lngValues.length; v++)
				objValues[v] = new Long(lngValues[v]);
		}
		else if (objClass.equals(float[].class)) {
			float[] fltValues = (float[]) values;
			objValues = new Object[fltValues.length];
			for (int v=0; v<fltValues.length; v++)
				objValues[v] = new Float(fltValues[v]);
		}
		else if (objClass.equals(double[].class)) {
			double[] dblValues = (double[]) values;
			objValues = new Object[dblValues.length];
			for (int v=0; v<dblValues.length; v++)
				objValues[v] = new Double(dblValues[v]);
		}
		else if (objClass.equals(double[].class)) {
			double[] dblValues = (double[]) values;
			objValues = new Object[dblValues.length];
			for (int v=0; v<dblValues.length; v++)
				objValues[v] = new Double(dblValues[v]);
		}
		else if (objClass.equals(Short[].class)) {
			Short[] shortValues = (Short[]) values;
			objValues = new Object[shortValues.length];
			for (int v=0; v<shortValues.length; v++)
				objValues[v] = shortValues[v];
		}
		else if (objClass.equals(Integer[].class)) {
			Integer[] integerValues = (Integer[]) values;
			objValues = new Object[integerValues.length];
			for (int v=0; v<integerValues.length; v++)
				objValues[v] = integerValues[v];
		}
		else if (objClass.equals(Long[].class)) {
			Long[] longValues = (Long[]) values;
			objValues = new Object[longValues.length];
			for (int v=0; v<longValues.length; v++)
				objValues[v] = longValues[v];
		}
		else if (objClass.equals(Float[].class)) {
			Float[] floatValues = (Float[]) values;
			objValues = new Object[floatValues.length];
			for (int v=0; v<floatValues.length; v++)
				objValues[v] = floatValues[v];
		}
		else if (objClass.equals(Double[].class)) {
			Double[] doubleValues = (Double[]) values;
			objValues = new Object[doubleValues.length];
			for (int v=0; v<doubleValues.length; v++)
				objValues[v] = doubleValues[v];
		}
		else if (objClass.equals(BigDecimal[].class)) {
			BigDecimal[] decimalValues = (BigDecimal[]) values;
			objValues = new Object[decimalValues.length];
			for (int v=0; v<decimalValues.length; v++)
				objValues[v] = decimalValues[v];
		}
		else if (objClass.equals(Date[].class)) {
			Date[] dateValues = (Date[]) values;
			objValues = new Object[dateValues.length];
			for (int v=0; v<dateValues.length; v++)
				objValues[v] = dateValues[v];
		}
		else if (objClass.equals(Time[].class)) {
			Time[] timeValues = (Time[]) values;
			objValues = new Object[timeValues.length];
			for (int v=0; v<timeValues.length; v++)
				objValues[v] = timeValues[v];
		}
		else if (objClass.equals(Timestamp[].class)) {
			Timestamp[] timestampValues = (Timestamp[]) values;
			objValues = new Object[timestampValues.length];
			for (int v=0; v<timestampValues.length; v++)
				objValues[v] = timestampValues[v];
		}
		else if (objClass.equals(String[].class)) {
			String[] stringValues = (String[]) values;
			objValues = new Object[stringValues.length];
			for (int v=0; v<stringValues.length; v++)
				objValues[v] = stringValues[v];
		}
		else if (objClass.equals(Object[].class)) {
			objValues = (Object[]) values;
		}
		else
			throw new ClassCastException("Cannot cast from "+objClass.getName()+" to Object[]");
		return objValues;
	}
		
}