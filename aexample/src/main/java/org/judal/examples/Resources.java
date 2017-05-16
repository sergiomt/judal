package org.judal.examples;

import java.io.InputStream;

public class Resources {

	public static InputStream getResourceAsStream(final String fileName) {
		return Resources.class.getResourceAsStream(fileName);
	}

}
