package org.judal.storage;

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

import java.util.Date;

import org.judal.metadata.ColumnDef;

import java.sql.Timestamp;
import java.sql.Types;

@SuppressWarnings("serial")
public class Param extends ColumnDef {

	public enum Direction {
		IN, OUT;
	}

	private Object oVal;
	private Direction oDir;

	public Param(String sColName, int iType, int iPos, Object oValue) {
		super(sColName, iType, iPos);
		oVal = oValue;
		oDir = Direction.IN;
	}

	public Param(String sColFamily, String sColName, int iType, int iPos, Object oValue) {
		super(sColName, iType, iPos);
		setFamily(sColFamily);
		oVal = oValue;
		oDir = Direction.IN;
	}
	
	public Param(String sColName, int iPos, short iVal) {
		super(sColName, Types.SMALLINT, iPos);
		oVal = new Short(iVal);
		oDir = Direction.IN;
	}

	public Param(String sColName, int iPos, Short iVal) {
		super(sColName, Types.SMALLINT, iPos);
		oVal = iVal;
		oDir = Direction.IN;
	}

	public Param(String sColName, int iPos, int iVal) {
		super(sColName, Types.INTEGER, iPos);
		oVal = new Integer(iVal);
		oDir = Direction.IN;
	}

	public Param(String sColName, int iPos, Integer iVal) {
		super(sColName, Types.INTEGER, iPos);
		oVal = iVal;
		oDir = Direction.IN;
	}
	
	public Param(String sColName, int iPos, long lVal) {
		super(sColName, Types.BIGINT, iPos);
		oVal = new Long(lVal);
		oDir = Direction.IN;
	}

	public Param(String sColName, int iPos, Long lVal) {
		super(sColName, Types.BIGINT, iPos);
		oVal = lVal;
		oDir = Direction.IN;
	}
	
	public Param(String sColName, int iPos, float fVal) {
		super(sColName, Types.FLOAT, iPos);
		oVal = new Float(fVal);
		oDir = Direction.IN;
	}

	public Param(String sColName, int iPos, Float fVal) {
		super(sColName, Types.FLOAT, iPos);
		oVal = fVal;
		oDir = Direction.IN;
	}
	
	public Param(String sColName, int iPos, double dVal) {
		super(sColName, Types.DOUBLE, iPos);
		oVal = new Double(dVal);
		oDir = Direction.IN;
	}

	public Param(String sColName, int iPos, Double dVal) {
		super(sColName, Types.DOUBLE, iPos);
		oVal = dVal;
		oDir = Direction.IN;
	}
	
	public Param(String sColName, int iPos, Date dtVal) {
		super(sColName, Types.TIMESTAMP, iPos);
		if (dtVal==null)
			oVal = null;
		else
		  oVal = new Timestamp(dtVal.getTime());
		oDir = Direction.IN;
	}

	public Param(String sColName, int iPos, String sVal) {
		super(sColName, Types.VARCHAR, iPos);
		oVal = sVal;
		oDir = Direction.IN;
	}
	
	public Param(ColumnDef oCol, Object oObj) {
		super(oCol);
		oVal = oObj;
		oDir = Direction.IN;
	}
	
	public Object getValue() {
		return oVal;
	}
	
	public Direction getDirection() {
		return oDir;
	}

	public void setDirection(Direction dirInOut) {
		oDir = dirInOut;
	}
	
}