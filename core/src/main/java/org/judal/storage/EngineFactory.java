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
	
	public static ThreadLocal<DataSource> DefaultThreadDataSource;
	
	private static Hashtable<String, Class<Engine<? extends DataSource>>> engines = new Hashtable<String, Class<Engine<? extends DataSource>>>();
	
	@SuppressWarnings("unchecked")
	/**
	 * <p>Register an Engine implementation under a given name</p>
	 * @param engineName String Engine name.
	 * @param engineClassName String Name of an implementation of org.judal.storage.Engine
	 * @throws ClassNotFoundException
	 * @throws IllegalArgumentException If another Engine implementation is already registered under the same name
	 */
	public static void registerEngine(String engineName, String engineClassName) throws ClassNotFoundException, IllegalArgumentException {
		Class<Engine<? extends DataSource>> engineClass = engines.get(engineName);
		if (engineClass==null)
			engines.put(engineName, (Class<Engine<? extends DataSource>>) Class.forName(engineClassName));
		else if (engineClass.getName().equals(engineClassName))
			throw new IllegalArgumentException("EngineFactory.registerEngine() Engine " + engineName + " is already registered for class " + engineClassName);
	}
	
	/**
	 * <p>Get implementation for a given named Engine</p>
	 * @param engineName String
	 * @return Engine&lt;? extends DataSource&gt;
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws NullPointerException If engineName is <b>null</b> or empty String
	 * @throws IllegalArgumentException If no Engine has been previously registered with the given name
	 */
	public static Engine<? extends DataSource> getEngine(String engineName) throws InstantiationException, IllegalAccessException, NullPointerException, IllegalArgumentException {
		if (null==engineName)
			throw new NullPointerException("EngineFactory.getEngine() Engine name cannot be null");
		else if (engineName.length()==0)
			throw new NullPointerException("EngineFactory.getEngine() Engine name is required");
		Class<Engine<? extends DataSource>> engineClass = engines.get(engineName);
		if (null==engineClass)
			throw new IllegalArgumentException("EngineFactory.getEngine() Cannot find any Engine with name " + engineName);
		return engineClass.newInstance();
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
