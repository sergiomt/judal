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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.jdo.JDOException;

/**
* Interface for SchemaMetaData scanners.
* @author Sergio Montoro Ten
* @version 1.0
*/
public interface MetadataScanner {

	/**
	 * <p>Create an SchemaMetaData instance from an InputStream.</p>
	 * @param instrm InputStream
	 * @return SchemaMetaData
	 * @throws JDOException
	 * @throws IOException
	 */
	SchemaMetaData readMetadata(InputStream instrm) throws JDOException, IOException;

	/**
	 * <p>Write SchemaMetaData to an OutputStream</p>
	 * @param metadata SchemaMetaData
	 * @param out OutputStream
	 * @throws JDOException
	 * @throws IOException
	 */
	void writeMetadata(SchemaMetaData metadata, OutputStream out) throws JDOException, IOException;

	}
