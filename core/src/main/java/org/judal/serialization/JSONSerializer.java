package org.judal.serialization;

import java.io.IOException;

import static java.sql.Types.*;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.judal.metadata.ColumnDef;
import org.judal.storage.table.Record;

public class JSONSerializer extends StdSerializer<Record> {

	private static final long serialVersionUID = 1L;

	private String dateFormat, timeFormat, timeWithTimezoneFormat, datetimeFormat, timestampFormat, timestampWithTimezoneFormat;

	public JSONSerializer() {
		this(null);
	}

	public JSONSerializer(Class<Record> t) {
		super(t);
		dateFormat = "YYYY-MM-dd";
		timeFormat = "HH:mm:ss";
		timeWithTimezoneFormat = timeFormat + " Z";
		datetimeFormat = "YYYY-MM-dd HH:mm:ss";
		timestampFormat = "YYYY-MM-dd HH:mm:ss.SSS";
		timestampWithTimezoneFormat = timestampFormat + " Z";
	}

	public void serializeField(Record record, final String fieldName, JsonGenerator jgen) throws IOException {
		throw new IOException("No serializer available for field " + fieldName);
	}

	@Override
	public void serialize(Record rec, JsonGenerator jgen, SerializerProvider provider)
			throws IOException, JsonProcessingException {

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
				case TIMESTAMP:
					jgen.writeStringField(fieldName, rec.getDateFormated(fieldName, timestampFormat));
				case TIMESTAMP_WITH_TIMEZONE:
					jgen.writeStringField(fieldName, rec.getDateFormated(fieldName, timestampWithTimezoneFormat));
				case BINARY:
				case LONGVARBINARY:
				case BLOB:
					jgen.writeBinaryField(fieldName, rec.getBytes(fieldName));
				default:
					serializeField(rec, fieldName, jgen);
				}
			}
		}
		jgen.writeEndObject();
	}
}