package org.judal.cassandra;

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

import java.math.BigDecimal;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

import java.sql.Types;

import java.util.Date;
import java.util.ListIterator;

import javax.jdo.JDOException;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;

import org.judal.metadata.ColumnDef;
import org.judal.storage.table.Record;

public class ColumnConverter {

	private static void deserializeByteBuffer(Record oRec, String sCol, ByteBuffer oBbuff) throws JDOException {
		if (oBbuff.hasRemaining()) {
			ColumnDef oCol = oRec.getColumn(sCol);
			switch (oCol.getType()) {
			case Types.CLOB:
			case Types.CHAR:
			case Types.NCHAR:
			case Types.VARCHAR:
			case Types.NVARCHAR:
			case Types.LONGVARCHAR:
			case Types.LONGNVARCHAR:
				oRec.put(sCol, StringSerializer.get().fromByteBuffer(oBbuff));
				break;
			case Types.BIGINT:
				LongBuffer oLbuff = oBbuff.asLongBuffer();
				oRec.put(sCol, new Long(oLbuff.get()));
				break;
			case Types.INTEGER:
				IntBuffer oIbuff = oBbuff.asIntBuffer();
				oRec.put(sCol, new Integer(oIbuff.get()));
				break;
			case Types.SMALLINT:
				ShortBuffer oSbuff = oBbuff.asShortBuffer();
				oRec.put(sCol, new Short(oSbuff.get()));
				break;
			case Types.FLOAT:
				FloatBuffer oFbuff = oBbuff.asFloatBuffer();
				oRec.put(sCol, new Float(oFbuff.get()));
				break;
			case Types.DOUBLE:
				DoubleBuffer oDbuff = oBbuff.asDoubleBuffer();
				oRec.put(sCol, new Double(oDbuff.get()));
				break;
			case Types.DATE:
			case Types.TIMESTAMP:
				LongBuffer oDtuff = oBbuff.asLongBuffer();
				oRec.put(sCol, new Date(oDtuff.get()));
				break;
			case Types.BLOB:
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
			case Types.JAVA_OBJECT:
				oRec.put(sCol, ByteBufferSerializer.get().toBytes(oBbuff));
				break;
			case Types.NUMERIC:
			case Types.DECIMAL:
				oRec.put(sCol, BigDecimalSerializer.get().fromByteBuffer(oBbuff));
				break;
			default:
				throw new JDOException("Cannot deserialize ByteBuffer of type "+ColumnDef.typeName(oCol.getType()));
			}
		} // fi
	}

	public static Record convertHectorRowToRecordStr(Record oRec, Row<String, String, String> oRow) throws JDOException {
		ColumnSlice<String,String> oSlc = oRow.getColumnSlice();
		ListIterator<HColumn<String,String>> oIter = oSlc.getColumns().listIterator();	          
		while (oIter.hasNext()) {
			HColumn<String,String> oCol = oIter.next();
			deserializeByteBuffer(oRec, oCol.getName(), oCol.getValueBytes());
		} //wend
		return oRec;
	}

	public static Record convertHectorRowToRecordLng(Record oRec, Row<String, String, Long> oRow) throws JDOException {
		ColumnSlice<String,Long> oSlc = oRow.getColumnSlice();
		ListIterator<HColumn<String,Long>> oIter = oSlc.getColumns().listIterator();	          
		while (oIter.hasNext()) {
			HColumn<String,Long> oCol = oIter.next();
			deserializeByteBuffer(oRec, oCol.getName(), oCol.getValueBytes());
		} //wend
		return oRec;
	}

	public static Record convertHectorRowToRecordInt(Record oRec, Row<String, String, Integer> oRow) throws JDOException {
		ColumnSlice<String,Integer> oSlc = oRow.getColumnSlice();
		ListIterator<HColumn<String,Integer>> oIter = oSlc.getColumns().listIterator();	          
		while (oIter.hasNext()) {
			HColumn<String,Integer> oCol = oIter.next();
			deserializeByteBuffer(oRec, oCol.getName(), oCol.getValueBytes());
		} //wend
		return oRec;
	}

	public static Record convertHectorRowToRecordDbl(Record oRec, Row<String, String, Double> oRow) throws JDOException {
		ColumnSlice<String,Double> oSlc = oRow.getColumnSlice();
		ListIterator<HColumn<String,Double>> oIter = oSlc.getColumns().listIterator();	          
		while (oIter.hasNext()) {
			HColumn<String,Double> oCol = oIter.next();
			deserializeByteBuffer(oRec, oCol.getName(), oCol.getValueBytes());
		} //wend
		return oRec;
	}

	public static Record convertHectorRowToRecordFlt(Record oRec, Row<String, String, Float> oRow) throws JDOException {
		ColumnSlice<String,Float> oSlc = oRow.getColumnSlice();
		ListIterator<HColumn<String,Float>> oIter = oSlc.getColumns().listIterator();	          
		while (oIter.hasNext()) {
			HColumn<String,Float> oCol = oIter.next();
			deserializeByteBuffer(oRec, oCol.getName(), oCol.getValueBytes());
		} //wend
		return oRec;
	}

	public static Record convertHectorRowToRecordDec(Record oRec, Row<String, String, BigDecimal> oRow) throws JDOException {
		ColumnSlice<String,BigDecimal> oSlc = oRow.getColumnSlice();
		ListIterator<HColumn<String,BigDecimal>> oIter = oSlc.getColumns().listIterator();	          
		while (oIter.hasNext()) {
			HColumn<String,BigDecimal> oCol = oIter.next();
			deserializeByteBuffer(oRec, oCol.getName(), oCol.getValueBytes());
		} //wend
		return oRec;
	}
	
	public static Record convertHectorRowToRecordDate(Record oRec, Row<String, String, Date> oRow) throws JDOException {
		ColumnSlice<String,Date> oSlc = oRow.getColumnSlice();
		ListIterator<HColumn<String,Date>> oIter = oSlc.getColumns().listIterator();	          
		while (oIter.hasNext()) {
			HColumn<String,Date> oCol = oIter.next();
			deserializeByteBuffer(oRec, oCol.getName(), oCol.getValueBytes());
		} //wend
		return oRec;
	}	
}
