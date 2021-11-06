package org.judal.halodb.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.judal.storage.Env;

public class TestHaloDB {

	public static Map<String,String> getTestProperties() throws IOException {
		InputStream inStrm = TestHaloDB.class.getResourceAsStream("datasource.properties");
		return Env.getDataSourceProperties(inStrm, "test");
	}

}