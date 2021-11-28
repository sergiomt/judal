package org.judal.serialization;

/*

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.*;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.judal.metadata.ColumnDef;
import org.judal.storage.table.Record;
*/

public class JSONSerializer { // extends StdSerializer<Record> {

	private static final long serialVersionUID = 1L;
	public JSONSerializer() {
	}

	/*

	private String dateFormat, timeFormat, timeWithTimezoneFormat, datetimeFormat, timestampFormat, timestampWithTimezoneFormat;

	private Map<String, DateFormat> dateFormaters;

	private Map<String, DateTimeFormatter> dateTimeFormaters;


	public JSONSerializer(Class<Record> t) {
		super(t);
		dateFormat = "YYYY-MM-dd";
		timeFormat = "HH:mm:ss";
		timeWithTimezoneFormat = timeFormat + " Z";
		datetimeFormat = "YYYY-MM-dd HH:mm:ss";
		timestampFormat = "YYYY-MM-dd HH:mm:ss.SSS";
		timestampWithTimezoneFormat = timestampFormat + " Z";
		dateFormaters = new HashMap<>();
		dateTimeFormaters = new HashMap<>();
	}

	public void serializeField(Record record, final String fieldName, JsonGenerator jgen) throws IOException {
		throw new IOException("No serializer available for field " + fieldName);
	}

	@Override
	public void serialize(Record rec, JsonGenerator jgen, SerializerProvider provider)
			throws IOException {

		jgen.writeStartObject();
		for (ColumnDef cdef : rec.columns()) {
			final String fieldName = cdef.getName();
			if (rec.isNull(fieldName))
				jgen.writeNullField(fieldName);
			else {
				switch (cdef.getType()) {
				case CHAR:
				case NCHAR:
				case VARCHAR:
				case NVARCHAR:
				case LONGVARCHAR:
				case LONGNVARCHAR:
				case CLOB:
					jgen.writeStringField(fieldName, rec.getString(fieldName));
					break;
				case TINYINT:
				case SMALLINT:
					jgen.writeNumberField(fieldName, rec.getShort(fieldName));
					break;
				case INTEGER:
					jgen.writeNumberField(fieldName, rec.getInteger(fieldName));
					break;
				case BIGINT:
					jgen.writeNumberField(fieldName, rec.getLong(fieldName));
					break;
				case FLOAT:
					jgen.writeNumberField(fieldName, rec.getFloat(fieldName));
					break;
				case DOUBLE:
					jgen.writeNumberField(fieldName, rec.getDouble(fieldName));
					break;
				case DECIMAL:
				case NUMERIC:
					jgen.writeNumberField(fieldName, rec.getDecimal(fieldName));
					break;
				case BOOLEAN:
					jgen.writeBooleanField(fieldName, rec.getBoolean(fieldName, false));
					break;
				case DATE:
					jgen.writeStringField(fieldName, rec.getDateFormated(fieldName, dateFormat));
					break;
				case TIMESTAMP:
					jgen.writeStringField(fieldName, rec.getDateFormated(fieldName, timestampFormat));
					break;
				case TIMESTAMP_WITH_TIMEZONE:
					jgen.writeStringField(fieldName, rec.getDateFormated(fieldName, timestampWithTimezoneFormat));
					break;
				case BINARY:
				case LONGVARBINARY:
				case BLOB:
					jgen.writeBinaryField(fieldName, rec.getBytes(fieldName));
					break;
				default:
					serializeField(rec, fieldName, jgen);
				}
			}
		}
		jgen.writeEndObject();
	}

	public void serialize(JsonGenerator jgen, String fieldName, Serializable fieldValue) throws IOException {
		if (fieldValue instanceof String) {
			jgen.writeStringField(fieldName, (String) fieldValue);
		} else if (fieldValue instanceof Integer) {
			jgen.writeNumberField(fieldName, (Integer) fieldValue);
		} else if (fieldValue instanceof Long) {
			jgen.writeNumberField(fieldName, (Long) fieldValue);
		} else if (fieldValue instanceof Short) {
			jgen.writeNumberField(fieldName, (Short) fieldValue);
		} else if (fieldValue instanceof Float) {
			jgen.writeNumberField(fieldName, (Float) fieldValue);
		} else if (fieldValue instanceof Double) {
			jgen.writeNumberField(fieldName, (Double) fieldValue);
		} else if (fieldValue instanceof BigDecimal) {
			jgen.writeNumberField(fieldName, (BigDecimal) fieldValue);
		} else if (fieldValue instanceof BigInteger) {
			jgen.writeNumberField(fieldName, new BigDecimal(((BigInteger) fieldValue).toString()));
		} else if (fieldValue instanceof Boolean) {
			jgen.writeBooleanField(fieldName, (Boolean) fieldValue);
		} else if (fieldValue instanceof java.sql.Date) {
			jgen.writeStringField(fieldName, getDateFormater(dateFormat).format((java.sql.Date) fieldValue));
		} else if (fieldValue instanceof Date) {
			jgen.writeStringField(fieldName, getDateFormater(datetimeFormat).format((Date) fieldValue));
		} else if (fieldValue instanceof LocalDate) {
			jgen.writeStringField(fieldName, ((LocalDate) fieldValue).format(getDateTimeFormater(dateFormat)));
		} else if (fieldValue instanceof LocalDateTime) {
			jgen.writeStringField(fieldName, ((LocalDateTime) fieldValue).format(getDateTimeFormater(datetimeFormat)));
		} else if (fieldValue instanceof ZonedDateTime) {
			jgen.writeStringField(fieldName, ((ZonedDateTime) fieldValue).format(getDateTimeFormater(timestampWithTimezoneFormat)));
		}
	}

	private DateFormat getDateFormater(String format) {
		DateFormat formater = dateFormaters.get(format);
		if (null==formater) {
			formater = new SimpleDateFormat(format);
			dateFormaters.put(format, formater);
		}
		return formater;
	}

	private DateTimeFormatter getDateTimeFormater(String format) {
		DateTimeFormatter formater = dateTimeFormaters.get(format);
		if (null==formater) {
			formater = new DateTimeFormatterBuilder().appendPattern(format).toFormatter();
			dateTimeFormaters.put(format, formater);
		}
		return formater;
	}
	 */
}