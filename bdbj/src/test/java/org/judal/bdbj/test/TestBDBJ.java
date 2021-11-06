package org.judal.bdbj.test;

import java.io.IOException;
import java.io.InputStream;

import java.util.Map;

import org.judal.storage.Env;

public class TestBDBJ {

	public Map<String,String> getTestProperties() throws IOException {
		InputStream inStrm = getClass().getResourceAsStream("datasource.properties");
		return Env.getDataSourceProperties(inStrm, "test");
	}

	
}
