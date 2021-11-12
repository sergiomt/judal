package org.judal.ramqueue;

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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.jdo.JDOException;
import javax.jms.JMSException;

import org.judal.storage.DataSource;
import org.judal.storage.Engine;
import org.judal.storage.queue.RecordQueueConsumer;

import com.knowgate.debug.DebugFile;

import org.judal.jms.ObjectMessageImpl;

/**
 * <p>Implement an in-memory RecordQueueConsumer using a ThreadPoolExecutor.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class RAMQueueConsumer extends ThreadPoolExecutor implements RecordQueueConsumer {

	private static final int DEFAULT_CORE_POOL_SIZE = 2;
	private static final int DEFAULT_MAX_POOL_SIZE = 2;
	private static final int DEFAULT_MAX_QUEUE_SIZE = 100000;
	
	/**
	 * <p>Constructor.</p>
	 * Create RAMQueueConsumer with DEFAULT_CORE_POOL_SIZE, DEFAULT_MAX_POOL_SIZE, 120 seconds keep alive time and a LinkedBlockingQueue
	 */
	public RAMQueueConsumer() {
		super(DEFAULT_CORE_POOL_SIZE, DEFAULT_MAX_POOL_SIZE, 120l, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(DEFAULT_MAX_QUEUE_SIZE));
	}

	/**
	 * <p>This method exists only for compliance with the RecordQueueConsumer interface but it does nothing.</p>
	 * @param engine Engine&lt;? extends DataSource&gt;
	 * @param properties Map&lt;String,String&gt;
	 * @throws JDOException Never thrown
	 */
	@Override
	public void start(Engine<? extends DataSource> engine, Map<String,String> properties) throws JDOException {
	}

	/**
	 * <p>Shutdown parent ThreadPoolExecutor.</p>
	 */
	@Override
	public void stop() throws JDOException {
		if (DebugFile.trace) {
		  DebugFile.writeln("Begin RAMQueueConsumer.stop()");
		  DebugFile.incIdent();
	    }

		super.shutdown();

		if (DebugFile.trace) {
		  DebugFile.decIdent();
		  DebugFile.writeln("End RAMQueueConsumer.stop()");
		}
	}

	/**
	 * <p>Forward call to ThreadPoolExecutor.execute(ObjectMessageImpl).</p>
	 * ThreadPoolExecutor will invoke ObjectMessageImpl.run() which is where
	 * the implementation of insert, update, store and delete operation is.
	 * @param oMsg ObjectMessageImpl
	 */
	public void onMessage(ObjectMessageImpl oMsg) {
	  if (DebugFile.trace) {
	    try {
	      DebugFile.writeln("RAMQueueConsumer.onMessage("+oMsg.getJMSMessageID()+","+oMsg+")");
      } catch (JMSException ignore) { }
	  }
	  super.execute(oMsg);
	}

}
