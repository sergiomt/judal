package org.judal.ramqueue;

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

public class RAMQueueConsumer extends ThreadPoolExecutor implements RecordQueueConsumer {

	private static final int DEFAULT_CORE_POOL_SIZE = 5;
	private static final int DEFAULT_MAX_POOL_SIZE = 10;
	private static final int DEFAULT_MAX_QUEUE_SIZE = 10000;
	
	public RAMQueueConsumer() {
		super(DEFAULT_CORE_POOL_SIZE, DEFAULT_MAX_POOL_SIZE, 120l, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(DEFAULT_MAX_QUEUE_SIZE));
	}

	@Override
	public void start(Engine<? extends DataSource> engine, Map<String,String> properties) throws JDOException {
	}

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

	public void onMessage(ObjectMessageImpl oMsg) {
	  if (DebugFile.trace) {
	    try {
	      DebugFile.writeln("RAMQueueConsumer.onMessage("+oMsg.getJMSMessageID()+","+oMsg+")");
      } catch (JMSException ignore) { }
	  }
	  super.execute(oMsg);
	}

}
