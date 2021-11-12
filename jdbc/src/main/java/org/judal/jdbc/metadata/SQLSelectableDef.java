package org.judal.jdbc.metadata;

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

import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.JDOUserException;

import org.judal.metadata.Scriptable;
import org.judal.metadata.SelectableDef;

public interface SQLSelectableDef extends SelectableDef, Scriptable {

	/**
	 * Get table names including joined tables if present
	 * @return String
	 */
	String getTables() throws JDOUserException,JDOUnsupportedOptionException;

	/**
	 *
	 * @return String
	 */
	String getCatalog();

	/**
	 *
	 * @return String
	 */
	String getSchema();

}