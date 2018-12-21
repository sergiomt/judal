package org.judal.jdbc.jdc;

import com.knowgate.debug.DebugFile;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 */

/**
 * <p>Connection pool daemon thread</p>
 * This thread scans a ConnectionPool every given interval calling
 * ConnectionPool.reapConnections() for closing unused connections.
 */
public class JDCConnectionReaper extends Thread {

	/**
	 * Reference to reaped connection pool
	 */
	private JDCConnectionPool pool;

	/**
	 * Used to stop the Connection reaper thread
	 */
	private boolean keepruning;

	/**
	 * Connection Reaper call interval (default = 10 mins)
	 */
	private long delay=600000l;

	/**
	 * <p>Constructor</p>
	 * @param forpool JDCConnectionPool
	 */
	JDCConnectionReaper(JDCConnectionPool forpool) {
		pool = forpool;
		keepruning = true;
		try {
			checkAccess();
			setDaemon(true);
			setPriority(MIN_PRIORITY);
			setName("JDC-Connection-Reaper-" + getId());
		} catch (SecurityException ignore) { }
	}

	/**
	 * Get connection reaper call interval
	 * @return long Number of milliseconds between reaper calls
	 */
	public long getDelay() {
		return delay;
	}

	/**
	 * <p>Set connection reaper call interval</p>
	 * The default value is 10 minutes
	 * @param lDelay long Number of milliseconds between reaper calls
	 * @throws IllegalArgumentException if lDelay is less than 1000
	 */
	public void setDelay(long lDelay) throws IllegalArgumentException {
		if (lDelay<1000l && lDelay>0l)
			throw new IllegalArgumentException("ConnectionReaper delay cannot be smaller than 1000 miliseconds");
		delay=lDelay;
	}

	public void halt() {
		keepruning = false;
		interrupt();
	}

	/**
	 * Reap connections every n-minutes
	 */
	public void run() {
		if (DebugFile.trace)
			DebugFile.writeln("Begin JDCConnectionReaper.run() delay=" + delay);
		while (keepruning) {
			try {
				sleep(delay);
			} catch( InterruptedException e) { }
			if (keepruning) pool.reapConnections();
		} // wend
		if (DebugFile.trace)
			DebugFile.writeln("End JDCConnectionReaper.run()");
	} // run
} // ConnectionReaper
