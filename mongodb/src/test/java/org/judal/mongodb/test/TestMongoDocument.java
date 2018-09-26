package org.judal.mongodb.test;

import org.apache.xerces.impl.dv.util.Base64;
import org.bson.Document;

import org.judal.metadata.TableDef;
import org.judal.mongodb.MongoDocument;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class TestMongoDocument {

	@Test
	public void testMongoDocument() throws ClassCastException, ClassNotFoundException {
		@SuppressWarnings("deprecation")
		Date d888 = new Date(118,7,8,8,8,8);
		byte[] binData = new byte[] {1,2,3,4,5,6,7,8};
		StringBuilder json = new StringBuilder();
		json.append("{");
		json.append("\"strCol\":\"strValue\",");
		json.append("\"intCol\":123,");
		json.append("\"floatCol\":123.456,");
		json.append("\"doubleCol\":1.2E+4,");
		json.append("\"dateCol\":{ \"$date\":"+d888.getTime()+"},");
		json.append("\"base64Col\":\""+Base64.encode(binData)+"\",");
		json.append("\"strArr\":[\"A\",\"B\",\"C\"],");
		json.append("\"intArr\":[1,2,3]");
		json.append("}");
;
		TableDef tdef = new TableDef("test");
		MongoDocument doc = new MongoDocument(tdef);
		doc.setDocument(Document.parse(json.toString()));
		assertEquals("strValue", doc.getString("strCol"));
		assertEquals(new Integer(123), doc.getInteger("intCol"));
		assertTrue(new Float(123.456).equals(doc.getFloat("floatCol")));
		assertTrue(new Double("1.2E+4").equals(doc.getDouble("doubleCol")));
		assertEquals(d888, doc.getDate("dateCol"));
		assertArrayEquals(binData, doc.getBytes("base64Col"));
		assertArrayEquals(new String[] {"A","B","C"}, doc.getStringArray("strArr"));
		assertArrayEquals(new Integer[] {1,2,3}, doc.getIntegerArray("intArr"));
	}

}
