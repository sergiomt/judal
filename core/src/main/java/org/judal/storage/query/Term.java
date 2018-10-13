package org.judal.storage.query;

/**
 * © Copyright 2016 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;

import org.judal.metadata.NameAlias;

import java.io.Serializable;
import java.math.BigDecimal;

import java.sql.Time;
import java.sql.Timestamp;

/**
* <p>Predicate Term</p>
* @author Sergio Montoro Ten
* @version 1.0
*/
public abstract class Term implements Part,Serializable {

	private static final long serialVersionUID = 10000L;
	
	protected NameAlias oAliasedTable;
	protected String sColumn;
	protected String sNestedColumn;
	protected String sOper;
	protected Object[] aValues;
	protected int nValues;
	
	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be one of {"IS NULL","IS NOT NULL"}
	 * @throws ArrayIndexOutOfBoundsException if oColumnValue is an array
	 */
	public Term(String sColumnName, String sOperator) throws IllegalArgumentException {
		if (!sOperator.equalsIgnoreCase(Operator.ISNULL) && !sOperator.equalsIgnoreCase(Operator.ISNOTNULL))
			throw new IllegalArgumentException("Operator must be either "+Operator.ISNULL+" or "+Operator.ISNOTNULL);
		oAliasedTable = null;
		sColumn = sColumnName;
		sNestedColumn = null;
		sOper = sOperator;	
	}

	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be one of {"=","&lt;&gt;","&gt;=","&lt;=","IS","IS NOT","IS NULL","IS NOT NULL","LIKE","ILIKE","BETWEEN","EXISTS","NOT EXISTS"}
	 * @param oColumnValue Object column value
	 * @throws ArrayIndexOutOfBoundsException if oColumnValue is an array
	 */
	public Term(String sColumnName, String sOperator, Object oColumnValue) throws ArrayIndexOutOfBoundsException {

		oAliasedTable = null;
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
		oAliasedTable = new NameAlias(sTableName, null);
		sColumn = sColumnName;
		sNestedColumn = sColumnName;
		sOper = sOperator;
		aValues = new Object[]{oNestedTerm};
		nValues = 1;
	}

	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be one of {"IN","NOT IN","EXISTS","NOT EXISTS","LIKE","ILIKE","BETWEEN"}
	 * @param oAliasedTableName NameAlias Aliased Table Name
	 * @param oNestedTerm QueryTerm Nested Query term
	 */
	public Term(String sColumnName, String sOperator, NameAlias oAliasedTableName, Part oNestedTerm) {
		oAliasedTable = oAliasedTableName;
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
		oAliasedTable = new NameAlias(sTableName,null);
		sColumn = sColumnName;
		sNestedColumn = sNestedColumnName;
		sOper = sOperator;
		aValues = new Object[]{oNestedTerm};
		nValues = 1;
	}

	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Must be Operator.BETWEEN or Operator.IN or Operator.NOTIN
	 * @param aColumnValues Collection&lt;Object&gt; column values
	 */
	public Term(String sColumnName, String sOperator, Collection<Object> aColumnValues) {
		oAliasedTable = null;
		sColumn = sColumnName;
		sOper = sOperator;
		nValues = aColumnValues.size();
		aValues = new Object[nValues];
		int v = 0;
		for (Object o :aColumnValues)
			aValues[v++] = o;
	}

	/**
	 * Create term.
	 * @param sColumnName String Name of column at outer table
	 * @param sOperator String Operator. Must be one of {"IN","NOT IN","EXISTS","NOT EXISTS","LIKE","ILIKE","BETWEEN"}
	 * @param oAliasedTableName NameAlias Aliased Table Name
	 * @param sNestedColumnName String Name of column at nested table
	 * @param oNestedTerm QueryTerm Nested Query term
	 */
	public Term(String sColumnName, String sOperator, NameAlias oAliasedTableName, String sNestedColumnName, Part oNestedTerm) {
		oAliasedTable = oAliasedTableName;
		sColumn = sColumnName;
		sNestedColumn = sNestedColumnName;
		sOper = sOperator;
		aValues = new Object[]{oNestedTerm};
		nValues = 1;
	}
	
	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Must be Operator.BETWEEN or Operator.IN or Operator.NOTIN
	 * @param aColumnValues String[] column values
	 */
	public Term(String sColumnName, String sOperator, String[] aColumnValues) {
	  oAliasedTable = null;
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
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN or Operator.NOTIN
	 * @param aColumnValues Integer[] column values
	 */
	public Term(String sColumnName, String sOperator, Integer[] aColumnValues) {
	  oAliasedTable = null;
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
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN or Operator.NOTIN
	 * @param aColumnValues int[] column values
	 */
	public Term(String sColumnName, String sOperator, int[] aColumnValues) {
	  oAliasedTable = null;
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
	 * @param aColumnValues Long[] column values
	 */
	public Term(String sColumnName, String sOperator, Long[] aColumnValues) {
	  oAliasedTable = null;
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
	 * @param aColumnValues long[] column values
	 */
	public Term(String sColumnName, String sOperator, long[] aColumnValues) {
		  oAliasedTable = null;
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
	 * @param aColumnValues Float[] column values
	 */
	public Term(String sColumnName, String sOperator, Float[] aColumnValues) {
		  oAliasedTable = null;
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
	 * @param aColumnValues Float[] column values
	 */
	public Term(String sColumnName, String sOperator, float[] aColumnValues) {
		  oAliasedTable = null;
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
	 * @param aColumnValues Double[] column values
	 */
	public Term(String sColumnName, String sOperator, Double[] aColumnValues) {
		  oAliasedTable = null;
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
	 * @param aColumnValues double[] column values
	 */
	public Term(String sColumnName, String sOperator, double[] aColumnValues) {
		  oAliasedTable = null;
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
	 * @param d1 double value 1
	 * @param d2 double value 2
	 * @param d3 double value 3
	 */
	public Term(String sColumnName, String sOperator, double d1, double d2, double d3) {
		  oAliasedTable = null;
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
	 * @param aColumnValues java.util.Date[] column values
	 */
	public Term(String sColumnName, String sOperator, Date[] aColumnValues) {
		  oAliasedTable = null;
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
	 * @param aColumnValues java.sql.Date[] column values
	 */
	public Term(String sColumnName, String sOperator, Calendar[] aColumnValues) {
		  oAliasedTable = null;
		sColumn = sColumnName;
		sOper = sOperator;
		nValues = aColumnValues.length;
		aValues = new Object[nValues];
		for (int v=0; v<nValues; v++)
		  aValues[v] = aColumnValues[v]==null ? null : new Date(aColumnValues[v].getTimeInMillis());
	}
	
	/**
	 * Create term.
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator. Must be Operator.BETWEEN or Operator.IN or Operator.WITHIN
	 * @param aColumnValues java.sql.Date[] column values
	 */
	public Term(String sColumnName, String sOperator, java.sql.Date[] aColumnValues) {
		  oAliasedTable = null;
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
	 * @param aColumnValues Timestamp[] column values
	 */
	public Term(String sColumnName, String sOperator, Timestamp[] aColumnValues) {
		  oAliasedTable = null;
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
	 * @param aColumnValues BigDecimal[] column values
	 */
	public Term(String sColumnName, String sOperator, BigDecimal[] aColumnValues) {
		  oAliasedTable = null;
		sColumn = sColumnName;
		sOper = sOperator;
		nValues = aColumnValues.length;
		aValues = new Object[nValues];
		for (int v=0; v<nValues; v++)
		  aValues[v] = aColumnValues[v];
	}
	
	protected Term() { }

	public abstract String getTableName();

	public abstract Term clone();

	protected void clone(Term source) {
		oAliasedTable = source.oAliasedTable;
		sColumn = source.sColumn;
		sNestedColumn = source.sNestedColumn;
		sOper = source.sOper;
		nValues = source.nValues;		
		if (source.aValues==null)
			aValues = null;
		else
			aValues = Arrays.copyOf(source.aValues, source.aValues.length);
	}

	public String getColumnName() {
		return sColumn;
	}

	/**
	 * @return One of org.judal.storage.query.Operator constants
	 * @see org.judal.storage.query.Operator
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
		else if (objClass.equals(Expression.class)) {
			objValues = (Expression[]) values;
		}
		else if (objClass.equals(Object[].class)) {
			objValues = (Object[]) values;
		}
		else
			throw new ClassCastException("Cannot cast from "+objClass.getName()+" to Object[]");
		return objValues;
	}
		
}