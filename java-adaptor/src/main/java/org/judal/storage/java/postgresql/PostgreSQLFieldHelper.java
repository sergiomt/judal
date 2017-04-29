package org.judal.storage.java.postgresql;

import java.util.Date;

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

import java.util.Map;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.knowgate.arrayutils.Arr;
import com.knowgate.gis.LatLong;
import com.knowgate.stringutils.Str;

import org.judal.storage.FieldHelper;
import org.judal.storage.table.Record;
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

public class PostgreSQLFieldHelper implements FieldHelper {

	private Constructor<?> oCnr = null;

	@SuppressWarnings("unchecked")
	private <T> T[] getArray(Object oObj) throws ClassCastException {
		if (null!=oObj) {
			if (oObj instanceof Array) {
				Array oArr = (Array) oObj;
				try {
					return (T[]) oArr.getArray();
				} catch (SQLException e) {
					throw new ClassCastException(e.getMessage());
				}				
			} else {
				return (T[]) oObj;
			}
		} else {
			return null;
		}		  
	}
	
	public Integer[] getIntegerArray(Record oRec, String sKey) throws ClassCastException {
		Object oObj = oRec.apply(sKey);
		return oObj instanceof int[] ? Arr.toObject((int[]) oObj) : (Integer[]) getArray(oObj);
	}

	public Long[] getLongArray(Record oRec, String sKey) throws ClassCastException {
		Object oObj = oRec.apply(sKey);
		return oObj instanceof long[] ? Arr.toObject((long[]) oObj) : (Long[]) getArray(oObj);
	}

	public Float[] getFloatArray(Record oRec, String sKey) throws ClassCastException {
		Object oObj = oRec.apply(sKey);
		return oObj instanceof float[] ? Arr.toObject((float[]) oObj) : (Float[]) getArray(oObj);
	}

	public Double[] getDoubleArray(Record oRec, String sKey) throws ClassCastException {
		Object oObj = oRec.apply(sKey);
		return oObj instanceof double[] ? Arr.toObject((double[]) oObj) : (Double[]) getArray(oObj);
	}

	public Date[] getDateArray(Record oRec, String sKey) throws ClassCastException {
		Object oObj = oRec.apply(sKey);
		if (null==oObj)
			return null;
		if (oObj instanceof Date[])
			return (Date[]) oObj;
		if (oObj instanceof Array) {
			Object oArr;
			try {
				oArr = ((Array) oObj).getArray();
			} catch (SQLException e) {
				throw new ClassCastException(e.getMessage());
			}
			if (oArr instanceof Date[]) {
				return (Date[]) oArr;
			}
			if (oArr instanceof Timestamp[]) {
				Timestamp[] oTms = (Timestamp[]) oArr;
				Date[] oDte = new Date[oTms.length];
				for (int t=0; t<oTms.length; t++)
					oDte[t] = new Date(oTms[t].getTime());
				return oDte;
			}
			if (oArr instanceof java.sql.Date[]) {
				java.sql.Date[] oSdt = (java.sql.Date[]) oArr;
				Date[] oDte = new Date[oSdt.length];
				for (int t=0; t<oSdt.length; t++)
					oDte[t] = new Date(oSdt[t].getYear(), oSdt[t].getMonth(), oSdt[t].getDate());
				return oDte;
			}
		}
		throw new ClassCastException("PostgreSQLFieldHelper.getDateArray() cannot cast from "+oObj.getClass().getName()+" to Date[]");
	}
	
	public String[] getStringArray(Record oRec, String sKey) throws ClassCastException {
		Object oObj = oRec.apply(sKey);
		if (null!=oObj) {
			if (oObj instanceof Array) {
				Array oArr = (Array) oRec.apply(sKey);
				try {
					return (String[]) oArr.getArray();
				} catch (SQLException e) {
					throw new ClassCastException(e.getMessage());
				}
			} else {
				return (String[]) oObj;
			}
		} else {
			return null;
		}		  
	}

	  /**
	   * <p>Get a part of an interval value</p>
	   * This function only works for PostgreSQL
	   * @param oRec Record
	   * @param sKey String String Field Name
	   * @param sPart String Currently, only "days" is allowed as interval part
	   * @return int Number of days in the given interval
	   * @throws NullPointerException if interval is <b>null</b>
	   * @throws IllegalArgumentException if sPart is not "days"
	   * @throws NumberFormatException if interval has no days
	   */
	public int getIntervalPart(Record oRec, String sKey, String sPart)
			throws ClassCastException, NullPointerException, NumberFormatException, IllegalArgumentException {
		if (sPart==null) throw new IllegalArgumentException("PostgreSQLFieldHelper.getIntervalPart() interval part to get cannot be null");
		if (!sPart.equalsIgnoreCase("days")) throw new IllegalArgumentException("PostgreSQLFieldHelper.getIntervalPart() interval part to get must be 'days'");
		Object oObj = oRec.apply(sKey);
		if (oObj==null) throw new NullPointerException("PostgreSQLFieldHelper.getIntervalPart() value of interval is null");
		String sTI = oObj.toString().toLowerCase();
		int iMons = sTI.indexOf("mons")<0 ? 0 : sTI.indexOf("mons")+4;
		int iDays = sTI.indexOf("days");
		if (iDays<0) return 0;
		return Integer.parseInt(Str.removeChars(sTI.substring(iMons,iDays), " "));
	} // getIntervalPart
	
	public LatLong getLatLong(Record oRec, String sKey) throws ClassCastException, NumberFormatException, ArrayIndexOutOfBoundsException {
		if (oRec.isNull(sKey)) {
			return null;
		} else if (oRec.apply(sKey) instanceof LatLong) {
			return (LatLong) oRec.apply(sKey);
		} else {
			String[] aLatLng = oRec.getString(sKey).split(" ");
			if (aLatLng.length!=2) throw new ArrayIndexOutOfBoundsException("PostgreSQLFieldHelper.getLatLong("+sKey+") bad geography format "+oRec.getString(sKey));
			return new LatLong(Float.parseFloat(aLatLng[0]), Float.parseFloat(aLatLng[1]));
		}
	}

	/**
	 * <p>Get value of an HStore field<p>
	 * This method is only supported for PostgreSQL HStore fields
	 * @param sKey JavaRecord Record instance
	 * @param sKey String Field Name
	 * @return Field value or <b>null</b>.
	 * @throws ClassNotFoundException 
	 * @throws SecurityException 
	 * @throws NoSuchMethodException 
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public Map<String, String> getMap(Record oRec, String sKey)
			throws NoSuchMethodException, SecurityException, ClassNotFoundException, InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		if (oRec.isNull(sKey)) {
			return null;
		} else {
			Object oObj = oRec.apply(sKey);
			if (oObj instanceof Map) {
				// Do nothing
			} else {
				if (null==oCnr)
					oCnr = Class.forName("org.judal.jdbc.HStore").getConstructor(String.class);
				Object oItr = oCnr.newInstance(oObj.toString());
				oObj = oItr.getClass().getMethod("asMap").invoke(oItr);
			}
			return (Map<String, String>) oObj;
		}
	} // getMap

}