package org.judal.metadata;

/**
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

import org.judal.storage.Param;

/**
 * Base class for procedure and trigger metadata definitions
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public abstract class CallableDef extends ExtendableDef implements Scriptable {

	private static final long serialVersionUID = 10000l;

	private String name;
	private String source;
	private String drop;
	private Param[] params;
	
	public CallableDef() { }

	/**
	 * <p>Set callable object name</p>
	 * @param name String
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * <p>Get callable object name</p>
	 * @return String
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * <p>Set callable object source code.</p>
	 * This source code is dependent on the data source where the callable object is instantiated.
	 * @param source String
	 */
	public void setSource(String source) {
		this.source = source;
	}

	/**
	 * <p>Get callable object source code.</p>
	 * @return String
	 */
	@Override
	public String getSource() {
		return source;
	}

	/**
	 * <p>Set source code needed to drop the callable object.</p>
	 * @param drop String For example "DROP PROCEDURE proc_name"
	 */
	public void setDrop(String drop) {
		this.drop = drop;
	}

	/**
	 * <p>Get source code needed to drop the callable object.</p>
	 * @return String
	 */
	@Override
	public String getDrop() {
		return drop;
	}
	
	/**
	 * <p>Callable object parameters definition.</p>
	 * @param params Param[]
	 */
	public void setParams(Param[] params) {
		this.params = params;
	}

	/**
	 * <p>Get source callable object parameter definitions.</p>
	 * @return Param[]
	 */
	public Param[] getParams() {
		return params;
	}
	
}
