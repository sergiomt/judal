package org.judal.metadata;

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

import org.judal.storage.Param;

/**
 * Base interface for objects that can be described by a script from a third party storage system.
 * @author Sergio Montoro Ten
 *
 */
public interface Scriptable {

	/**
	 * @return String Object name
	 */
	String getName();
	
	/**
	 * @return Param[] Parameter definition for callable objects
	 */
	Param[] getParams();

	/**
	 * Get source code to create this object.
	 * This value will depend on the storage system used.
	 * @return String
	 */
	String getSource();
	
	/**
	 * Get source code to drop this object.
	 * This value will depend on the storage system used.
	 * @return String
	 */
	String getDrop();

}
