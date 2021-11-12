package org.judal.storage.queue;

/*
 * © Copyright 2016 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.util.Map;

import javax.jdo.JDOException;

import org.judal.storage.DataSource;
import org.judal.storage.Engine;

/**
 * <p>Record queue consumer.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface RecordQueueConsumer {

	/**
	 * <p>Start record queue consumer.</p>
	 * @param engine Engine&lt;? extends DataSource&gt;
	 * @param properties Map&lt;String,String&gt;
	 * @throws JDOException
	 */
	void start(Engine<? extends DataSource> engine, Map<String,String> properties) throws JDOException;
	
	/**
	 * @throws JDOException
	 */
	void stop() throws JDOException;
	
}
