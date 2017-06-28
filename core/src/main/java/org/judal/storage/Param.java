package org.judal.storage;

import java.util.Calendar;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import org.judal.metadata.ColumnDef;
import org.judal.storage.query.Expression;

import java.sql.Timestamp;
import java.sql.Types;

/**
 * <p>Input or output parameter for insert, update, exist or fetch methods.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
@SuppressWarnings("serial")
public class Param extends ColumnDef {

	public enum Direction {
		IN, OUT;
	}

	private Object oVal;
	private Direction oDir;

	/**
	 * <p>Constructor</p>
	 * @param sColName String Column name
	 * @param iType int Column type one of java.sql.Types
	 * @param iPos int Position [1..n]
	 * @param oValue Object Value of parameter
	 */
	public Param(String sColName, int iType, int iPos, Object oValue) {
		super(sColName, iType, iPos);
		oVal = oValue;
		oDir = Direction.IN;
	}
	
	/**
	 * <p>Constructor</p>
	 * @param sColName String Column name
	 * @param iType int Column type one of java.sql.Types
	 * @param iPos int Position [1..n]
	 * @param dirInOut Direction
	 * @param oValue Object Value of parameter
	 */
	public Param(String sColName, int iType, int iPos, Direction dirInOut, Object oValue) {
		super(sColName, iType, iPos);
		oVal = oValue;
		oDir = dirInOut;
	}

	/**
	 * <p>Constructor</p>
	 * @param sColName String Column name
	 * @param iType int Column type one of java.sql.Types
	 * @param iPos int Position [1..n]
	 * @param dirInOut Direction
	 */
	public Param(String sColName, int iType, int iPos, Direction dirInOut) {
		super(sColName, iType, iPos);
		oDir = dirInOut;
	}
	
	/**
	 * <p>Constructor</p>
	 * @param sColFamily String Column family name
	 * @param sColName String Column name
	 * @param iType int Column type one of java.sql.Types
	 * @param iPos int Position [1..n]
	 * @param oValue Object Value of parameter
	 */
	public Param(String sColFamily, String sColName, int iType, int iPos, Object oValue) {
		super(sColName, iType, iPos);
		setFamily(sColFamily);
		oVal = oValue;
		oDir = Direction.IN;
	}
	
	/**
	 * <p>Constructor</p>
	 * @param sColFamily String Column family name
	 * @param sColName String Column name
	 * @param iType int Column type one of java.sql.Types
	 * @param iPos int Position [1..n]
	 * @param dirInOut Direction
	 */
	public Param(String sColFamily, String sColName, int iType, int iPos, Direction dirInOut) {
		super(sColName, iType, iPos);
		setFamily(sColFamily);
		oDir = dirInOut;
	}

	/**
	 * <p>Constructor</p>
	 * @param sColFamily String Column family name
	 * @param sColName String Column name
	 * @param iType int Column type one of java.sql.Types
	 * @param iPos int Position [1..n]
	 * @param dirInOut Direction
	 * @param oValue Object Value of parameter
	 */
	public Param(String sColFamily, String sColName, int iType, int iPos, Direction dirInOut, Object oValue) {
		super(sColName, iType, iPos);
		setFamily(sColFamily);
		oVal = oValue;
		oDir = dirInOut;
	}

	/**
	 * <p>Constructor for input short value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param iVal short
	 */
	public Param(String sColName, int iPos, short iVal) {
		super(sColName, Types.SMALLINT, iPos);
		oVal = new Short(iVal);
		oDir = Direction.IN;
	}

	/**
	 * <p>Constructor for input Short value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param iVal Short
	 */
	public Param(String sColName, int iPos, Short iVal) {
		super(sColName, Types.SMALLINT, iPos);
		oVal = iVal;
		oDir = Direction.IN;
	}

	/**
	 * <p>Constructor for input int value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param iVal int
	 */
	public Param(String sColName, int iPos, int iVal) {
		super(sColName, Types.INTEGER, iPos);
		oVal = new Integer(iVal);
		oDir = Direction.IN;
	}

	/**
	 * <p>Constructor for input Integer value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param iVal Integer
	 */
	public Param(String sColName, int iPos, Integer iVal) {
		super(sColName, Types.INTEGER, iPos);
		oVal = iVal;
		oDir = Direction.IN;
	}
	
	/**
	 * <p>Constructor for input long value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param lVal long
	 */
	public Param(String sColName, int iPos, long lVal) {
		super(sColName, Types.BIGINT, iPos);
		oVal = new Long(lVal);
		oDir = Direction.IN;
	}

	/**
	 * <p>Constructor for input Long value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param lVal Long
	 */
	public Param(String sColName, int iPos, Long lVal) {
		super(sColName, Types.BIGINT, iPos);
		oVal = lVal;
		oDir = Direction.IN;
	}
	
	/**
	 * <p>Constructor for input float value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param fVal float
	 */
	public Param(String sColName, int iPos, float fVal) {
		super(sColName, Types.FLOAT, iPos);
		oVal = new Float(fVal);
		oDir = Direction.IN;
	}

	/**
	 * <p>Constructor for input Float value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param fVal Float
	 */
	public Param(String sColName, int iPos, Float fVal) {
		super(sColName, Types.FLOAT, iPos);
		oVal = fVal;
		oDir = Direction.IN;
	}
	
	/**
	 * <p>Constructor for input double value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param dVal double
	 */
	public Param(String sColName, int iPos, double dVal) {
		super(sColName, Types.DOUBLE, iPos);
		oVal = new Double(dVal);
		oDir = Direction.IN;
	}

	/**
	 * <p>Constructor for input Double value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param dVal Double
	 */
	public Param(String sColName, int iPos, Double dVal) {
		super(sColName, Types.DOUBLE, iPos);
		oVal = dVal;
		oDir = Direction.IN;
	}
	
	/**
	 * <p>Constructor for input timestamp value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param dtVal java.util.Date
	 */
	public Param(String sColName, int iPos, java.util.Date dtVal) {
		super(sColName, Types.TIMESTAMP, iPos);
		if (dtVal==null)
			oVal = null;
		else
		  oVal = new Timestamp(dtVal.getTime());
		oDir = Direction.IN;
	}

	/**
	 * <p>Constructor for input timestamp value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param dtVal java.util.Calendar
	 */
	public Param(String sColName, int iPos, Calendar dtVal) {
		super(sColName, Types.TIMESTAMP, iPos);
		if (dtVal==null)
			oVal = null;
		else
		  oVal = new Timestamp(dtVal.getTimeInMillis());
		oDir = Direction.IN;
	}

	/**
	 * <p>Constructor for input timestamp value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param tsVal java.sql.Timestamp
	 */
	public Param(String sColName, int iPos, Timestamp tsVal) {
		super(sColName, Types.TIMESTAMP, iPos);
		if (tsVal==null)
			oVal = null;
		else
		  oVal = tsVal;
		oDir = Direction.IN;
	}

	/**
	 * <p>Constructor for input date value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param dtVal java.sql.Date
	 */
	public Param(String sColName, int iPos, java.sql.Date dtVal) {
		super(sColName, Types.DATE, iPos);
		if (dtVal==null)
			oVal = null;
		else
		  oVal = dtVal;
		oDir = Direction.IN;
	}

	/**
	 * <p>Constructor for input string value</p>
	 * @param sColName String Column name
	 * @param iPos int Position [1..n]
	 * @param sVal String
	 */
	public Param(String sColName, int iPos, String sVal) {
		super(sColName, Types.VARCHAR, iPos);
		oVal = sVal;
		oDir = Direction.IN;
	}
	
	/**
	 * <p>Constructor for input expression value</p>
	 * @param sColName String Column name
	 * @param iType int One of java.sql.Types
	 * @param iPos int Position [1..n]
	 * @param eVal Expression
	 */
	public Param(String sColName, int iType, int iPos, Expression eVal) {
		super(sColName, iType, iPos);
		oVal = eVal;
		oDir = Direction.IN;
	}

	/**
	 * <p>Constructor for input value</p>
	 * @param oCol ColumnDef
	 * @param oObj Object
	 */
	public Param(ColumnDef oCol, Object oObj) {
		super(oCol);
		oVal = oObj;
		oDir = Direction.IN;
	}
	
	/**
	 * <p>Get this Param value.</p>
	 * @return Object
	 */
	public Object getValue() {
		return oVal;
	}

	/**
	 * <p>Set this Param value.</p>
	 * @param oObj Object
	 */
	public void setValue(Object oObj) {
		oVal = oObj;
	}

	/**
	 * <p>Get this Param Direction.</p>
	 * @return Direction
	 */
	public Direction getDirection() {
		return oDir;
	}

	/**
	 * <p>Set this Param Direction.</p>
	 * @param dirInOut Direction
	 */
	public void setDirection(Direction dirInOut) {
		oDir = dirInOut;
	}
	
}