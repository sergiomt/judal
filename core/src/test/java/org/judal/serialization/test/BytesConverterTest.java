package org.judal.serialization.test;

import java.io.IOException;
import java.sql.Types;
import java.util.HashMap;

import org.judal.serialization.BytesConverter;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class BytesConverterTest {

	@Test
	public void test01IntegerToBytes() throws IOException {
		final String s = "113";
		final Integer i = new Integer(113);
		final byte [] fromInteger = BytesConverter.toBytes(i);
		final byte [] fromIntInt = BytesConverter.toBytes(i, Types.INTEGER);
		final byte [] fromString = BytesConverter.toBytes(s, Types.INTEGER);

		assertArrayEquals(fromInteger, fromIntInt);
		assertArrayEquals(fromInteger, fromString);

		Object o = BytesConverter.fromBytes(fromInteger, Types.INTEGER);
		
		assertTrue(o instanceof Integer);

		assertEquals(s, o.toString());
	}

	@Test
	public void test02StringToBytes() throws IOException {
		final String s = "Üñicode String";
		final byte [] fromString = BytesConverter.toBytes(s);
		final byte [] fromStrStr = BytesConverter.toBytes(s, Types.VARCHAR);

		assertArrayEquals(fromString, fromStrStr);

		Object o = BytesConverter.fromBytes(fromStrStr, Types.VARCHAR);
		
		assertTrue(o instanceof String);

		assertEquals(s, (String) o);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void test03ObjectToBytes() throws IOException {
		final HashMap<Integer,String> h = new HashMap<Integer,String>();
		h.put(1, "one");
		h.put(2, "two");
		final byte [] fromObject = BytesConverter.toBytes(h);
		final byte [] fromObjObj = BytesConverter.toBytes(h, Types.JAVA_OBJECT);

		assertArrayEquals(fromObject, fromObjObj);

		Object o = BytesConverter.fromBytes(fromObjObj, Types.JAVA_OBJECT);
		
		assertTrue(o instanceof HashMap);

		assertEquals("one", ((HashMap<Integer,String>)o).get(1));
	}
	
}
