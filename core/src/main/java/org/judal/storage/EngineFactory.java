package org.judal.storage;

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

import java.util.Hashtable;

public class EngineFactory {
	
	private static Hashtable<String, Class<Engine<? extends DataSource>>> engines = new Hashtable<String, Class<Engine<? extends DataSource>>>();
	
	@SuppressWarnings("unchecked")
	public static void registerEngine(String engineName, String engineClassName) throws ClassNotFoundException {
		engines.put(engineName, (Class<Engine<? extends DataSource>>) Class.forName(engineClassName));
	}
	
	public static Engine<? extends DataSource> getEngine(String engineName) throws InstantiationException, IllegalAccessException {
		return engines.get(engineName).newInstance();
	}

	/*
	 * Some standard names for engines
	 */
	public static final String NAME_JDBC = "JDBC";
	public static final String NAME_BERKELEYDB = "BDB";
	public static final String NAME_AMAZONS3 = "S3";
	public static final String NAME_CASSANDRA = "CASSANDRA";
	public static final String NAME_HBASE = "HBASE";
	public static final String NAME_MONGODB = "MONGODB";
	public static final String NAME_REDIS = "REDIS";
	
}
