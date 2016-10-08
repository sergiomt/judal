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

import javax.jdo.Extent;
import javax.jdo.JDOException;

public interface ReadOnlyBucket extends AutoCloseable, Extent<Stored> {

	String name();

	boolean exists(Object key) throws JDOException;

	boolean load(Object key, Stored target) throws JDOException;

	void close() throws JDOException;

	void setClass(Class<Stored> candidateClass);

	public static final int MAX_ROWS = 2147483647;
}
