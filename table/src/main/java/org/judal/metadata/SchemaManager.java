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

import java.io.PrintWriter;

import javax.jdo.JDOException;

/**
 * Interface for managing schemas from SchemaMetaData
 * @author Sergio Montoro Ten
 *
 */
public interface SchemaManager {

	/**
	 * <p>Create schema.</p>
	 * @param metadata SchemaMetaData
	 * @return int
	 * @throws JDOException
	 */
	int create(SchemaMetaData metadata) throws JDOException;

	/**
	 * <p>Update schema.</p>
	 * @param metadata SchemaMetaData
	 * @return int
	 * @throws JDOException
	 */
	int update(SchemaMetaData metadata) throws JDOException;

	/**
	 * <p>Drop schema.</p>
	 * @param metadata SchemaMetaData
	 * @return int
	 * @throws JDOException
	 */
	int drop  (SchemaMetaData metadata) throws JDOException;

	/**
	 * @param printer PrintWriter
	 * @throws JDOException
	 */
	void setLogWriter (PrintWriter printer);
	
	/**
	 * @return boolean
	 */
	boolean stopOnError();

	/**
	 * @param stopOnError boolean
	 */
	void stopOnError(boolean stopOnError);
	
}
