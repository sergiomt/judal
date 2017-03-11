package org.judal.serialization;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

/**
 * Convert Java objects to byte arrays and byte arrays to Java objects
 * @author Sergio Montoro Ten
 *
 */
public class BytesConverter {

	/**
	 * Convert a Java object to a bytes representation
	 * @param oObj Object
	 * @param iType java.sql.Type
	 * @return byte[]
	 */
	public static byte[] toBytes(Object oObj, int iType) {
		if (oObj==null) return new byte[0];
		switch (iType) {
		case Types.TINYINT:
			return new byte[]{((Byte)oObj).byteValue()};
		case Types.CHAR:
		case Types.NCHAR:
		case Types.VARCHAR:
		case Types.NVARCHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
			try {
				return oObj.toString().getBytes("UTF-8");
			} catch (UnsupportedEncodingException neverthrown) { }
		case Types.BOOLEAN:
			if (oObj instanceof Boolean)
				return Bytes.toBytes(((Boolean) oObj).booleanValue());
			else
				return Bytes.toBytes(new Boolean(oObj.toString()).booleanValue());								
		case Types.SMALLINT:
			if (oObj instanceof Short)
				return Bytes.toBytes(((Short) oObj).shortValue());
			else if (oObj instanceof String)
				return Bytes.toBytes(new Short((String) oObj).shortValue());				
			else
				return Bytes.toBytes(new Short(oObj.toString()).shortValue());				
		case Types.INTEGER:
			if (oObj instanceof Integer)
				return Bytes.toBytes(((Integer) oObj).intValue());
			else if (oObj instanceof Short)
				return Bytes.toBytes(((Short) oObj).intValue());
			else if (oObj instanceof String)
				return Bytes.toBytes(new Integer((String) oObj).intValue());				
			else
				return Bytes.toBytes(new Integer(oObj.toString()).intValue());
		case Types.BIGINT:
			if (oObj instanceof Long)
				return Bytes.toBytes(((Long) oObj).longValue());
			else if (oObj instanceof Integer)
				return Bytes.toBytes(((Integer) oObj).longValue());
			else if (oObj instanceof Short)
				return Bytes.toBytes(((Short) oObj).longValue());
			else if (oObj instanceof String)
				return Bytes.toBytes(new Long((String) oObj).longValue());				
			else
				return Bytes.toBytes(new Long(oObj.toString()).longValue());
		case Types.FLOAT:
			if (oObj instanceof Float)
				return Bytes.toBytes(((Float) oObj).floatValue());
			else if (oObj instanceof Short)
				return Bytes.toBytes((float)((Short) oObj).shortValue());
			else if (oObj instanceof Integer)
				return Bytes.toBytes((float)((Integer) oObj).intValue());
			else if (oObj instanceof Long)
				return Bytes.toBytes((float)((Long) oObj).longValue());
			else if (oObj instanceof Double)
				return Bytes.toBytes(((Double) oObj).floatValue());
			else if (oObj instanceof String)
				return Bytes.toBytes(new Float((String) oObj).floatValue());				
			else 
				return Bytes.toBytes(new Float(oObj.toString()).floatValue());
		case Types.DOUBLE:
			if (oObj instanceof Double)
				return Bytes.toBytes(((Double) oObj).doubleValue());
			else if (oObj instanceof Short)
				return Bytes.toBytes((double)((Short) oObj).shortValue());
			else if (oObj instanceof Integer)
				return Bytes.toBytes((double)((Integer) oObj).intValue());
			else if (oObj instanceof Long)
				return Bytes.toBytes((double)((Long) oObj).longValue());
			else if (oObj instanceof Float)
				return Bytes.toBytes((double)((Float) oObj).floatValue());
			else if (oObj instanceof String)
				return Bytes.toBytes(new Double((String) oObj).doubleValue());				
			else 
				return Bytes.toBytes(new Double(oObj.toString()).doubleValue());
		case Types.DECIMAL:
		case Types.NUMERIC:
			if (oObj instanceof BigDecimal)
				return Bytes.toBytes((BigDecimal) oObj);
			else if (oObj instanceof String)
				return Bytes.toBytes(new BigDecimal((String) oObj));
			else
				return Bytes.toBytes(new BigDecimal(oObj.toString()));
		case Types.TIMESTAMP:
		case Types.DATE:
			if (oObj instanceof Date)
				return Bytes.toBytes(((Date) oObj).getTime());
			else if (oObj instanceof Timestamp)
				return Bytes.toBytes(((Timestamp) oObj).getTime());		
			else if (oObj instanceof Long)
				return Bytes.toBytes(((Long) oObj).longValue());		
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			if (oObj instanceof byte[])	    
				return (byte[]) oObj;
			else if (oObj instanceof String)
				return toBytes(oObj, Types.VARCHAR);
			else if (oObj instanceof Byte)
				return toBytes(oObj, Types.TINYINT);
			else if (oObj instanceof Boolean)
				return toBytes(oObj, Types.BOOLEAN);
			else if (oObj instanceof Short)
				return toBytes(oObj, Types.SMALLINT);
			else if (oObj instanceof Integer)
				return toBytes(oObj, Types.INTEGER);				
			else if (oObj instanceof Long)
				return toBytes(oObj, Types.BIGINT);
			else if (oObj instanceof Float)
				return toBytes(oObj, Types.FLOAT);				
			else if (oObj instanceof Double)
				return toBytes(oObj, Types.DOUBLE);				
			else if (oObj instanceof BigDecimal)
				return toBytes(oObj, Types.DECIMAL);
			else if (oObj instanceof Date)
				return toBytes(oObj, Types.TIMESTAMP);
			else if (oObj instanceof Timestamp)
				return toBytes(oObj, Types.TIMESTAMP);
			else
				return toBytes(oObj, Types.JAVA_OBJECT);
		case Types.JAVA_OBJECT:
			ByteArrayOutputStream byOut = new ByteArrayOutputStream();
			ObjectOutputStream oOut;
			byte[] aRetVal = null;
			try {
				oOut = new ObjectOutputStream (byOut);
				oOut.writeObject(oObj);
				aRetVal = byOut.toByteArray();
				oOut.close();
				byOut.close();
			} catch (IOException ioe) {

				System.out.println("IOException serializing "+oObj.getClass().getName()+" "+ioe.getMessage());
				try {
					System.out.println(com.knowgate.debug.StackTraceUtil.getStackTrace(ioe));
				} catch (IOException e) { }
				throw new RuntimeException("BytesConverter.toBytes() IOException serializing "+oObj.getClass().getName()+" "+ioe.getMessage());
			}
			return aRetVal;				
		default:
			throw new RuntimeException("BytesConverter.toBytes() unrecognized type "+String.valueOf(iType));
		}	
	}

	/**
	 * Convert a Java object to a bytes representation
	 * @param oObj Object
	 * @return byte[]
	 */
	public static byte[] toBytes(Object oObj) {
		if (oObj==null) return new byte[0];
		if (oObj instanceof String)
			return toBytes(oObj, Types.VARCHAR);
		else if (oObj instanceof Boolean)
			return toBytes(oObj, Types.BOOLEAN);
		else if (oObj instanceof Short)
			return toBytes(oObj, Types.SMALLINT);
		else if (oObj instanceof Integer)
			return toBytes(oObj, Types.INTEGER);
		else if (oObj instanceof Long)
			return toBytes(oObj, Types.BIGINT);
		else if (oObj instanceof Float)
			return toBytes(oObj, Types.FLOAT);
		else if (oObj instanceof Double)
			return toBytes(oObj, Types.DOUBLE);
		else if (oObj instanceof BigDecimal)
			return toBytes(oObj, Types.DECIMAL);
		else if (oObj instanceof Date)
			return toBytes(oObj, Types.TIMESTAMP);
		else if (oObj instanceof byte[])
			return toBytes(oObj, Types.VARBINARY);
		else
			return toBytes(oObj, Types.JAVA_OBJECT);
	}
	
	/**
	 * Convert an input byte array into a Java object
	 * @param aBytes byte[]
	 * @return Object
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public static Object fromBytes(byte[] aBytes, int iType) throws IOException {
		if (aBytes==null) return null;
		switch (iType) {
		case Types.CHAR:
		case Types.NCHAR:
		case Types.VARCHAR:
		case Types.NVARCHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
		case Types.CLOB:
			if (aBytes.length==0)
				return "";
			else
				return Bytes.toString(aBytes);
		case Types.BOOLEAN:
			if (aBytes.length==0)
				return null;
			else
				return new Boolean(Bytes.toBoolean(aBytes));
		case Types.SMALLINT:
			if (aBytes.length==0)
				return null;
			else
				return new Short(Bytes.toShort(aBytes));
		case Types.INTEGER:
			if (aBytes.length==0)
				return null;
			else
				return new Integer(Bytes.toInt(aBytes));
		case Types.BIGINT:
			if (aBytes.length==0)
				return null;
			else
				return new Long(Bytes.toLong(aBytes));
		case Types.FLOAT:
			if (aBytes.length==0)
				return null;
			else
				return new Float(Bytes.toFloat(aBytes));
		case Types.DOUBLE:
			if (aBytes.length==0)
				return null;
			else
				return new Double(Bytes.toDouble(aBytes));
		case Types.DECIMAL:
		case Types.NUMERIC:
			if (aBytes.length==0)
				return null;
			else
				return Bytes.toBigDecimal(aBytes);
		case Types.TIMESTAMP:
		case Types.DATE:
			if (aBytes.length==0)
				return null;
			else
				return new Date(Bytes.toLong(aBytes));
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
		case Types.BLOB:
			return aBytes;
		case Types.JAVA_OBJECT:
			if (aBytes.length==0) {
				return null;
			} else {
				ByteArrayInputStream byIn = new ByteArrayInputStream(aBytes);
				ObjectInputStream oIn = new ObjectInputStream (byIn);
				Object oRetVal;
				try {
					oRetVal = oIn.readObject();
				} catch (ClassNotFoundException cnf) {
					throw new IOException ("BytesConverter.fromBytes() ClassNotFoundException "+cnf.getMessage());
				} finally {
					oIn.close();
					byIn.close();      	
				}
				return oRetVal;
			}
		default:
			return aBytes;
		}
	} // fromBytes
	
}
