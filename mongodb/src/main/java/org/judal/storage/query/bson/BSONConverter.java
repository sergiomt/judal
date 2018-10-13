package org.judal.storage.query.bson;

import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonBoolean;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;

import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonDouble;

public class BSONConverter {

	public static BsonValue convert(Object obj) throws ClassCastException {
		if (null==obj)
			return new BsonNull();
		else if (obj instanceof String)
			return new BsonString((String) obj);
		else if (obj instanceof Integer)
			return new BsonInt32((Integer) obj);
		else if (obj instanceof Long)
			return new BsonInt64((Long) obj);
		else if (obj instanceof Float)
			return new BsonDouble((Float) obj);
		else if (obj instanceof Double)
			return new BsonDouble((Double) obj);
		else if (obj instanceof BigDecimal)
			return new BsonDecimal128(Decimal128.parse(((BigDecimal) obj).toString()));
		else if (obj instanceof Boolean)
			return new BsonBoolean((Boolean) obj);
		else if (obj instanceof Date)
			return new BsonDateTime(((Date) obj).getTime());
		else if (obj instanceof Calendar)
			return new BsonDateTime(((Calendar) obj).getTimeInMillis());
		else
			throw new ClassCastException("Cannot convert " + obj.getClass().getName()+" to BsonValue");
	}
}
