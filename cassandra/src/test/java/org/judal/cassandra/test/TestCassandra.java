package org.judal.cassandra.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.judal.storage.Env;

public class TestCassandra {

	public Map<String,String> getTestProperties() throws IOException {
		InputStream inStrm = getClass().getResourceAsStream("datasource.properties");
		return Env.getDataSourceProperties(inStrm, "test");
	}

	
}