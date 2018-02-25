package org.judal.metadata.bind;

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

import java.io.IOException;
import java.io.InputStream;
import java.io.FileNotFoundException;

import javax.jdo.JDOException;

import org.judal.metadata.MetadataScanner;
import org.judal.metadata.SchemaMetaData;
import org.judal.storage.DataSource;

import com.knowgate.debug.DebugFile;
import com.knowgate.stringutils.Str;

/**
 * <p>Load schema metadata from a JDO XML resource file of a package.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JdoPackageMetadata implements MetadataScanner {

	private DataSource dataSource;
	private String packagePath;
	private String xmlFileName;
	
 	/**
 	 * <p>Constructor.</p>
 	 * @param dataSource DataSource
 	 * @param packagePath String A package path composed of names separated by forward slash like "com/acme/app/model"
 	 * @param xmlFileName String Optional. Name (with extension) of XML file present as a resource in packagePath
 	 */
 	public JdoPackageMetadata(DataSource dataSource, String packagePath, String xmlFileName) {
 		this.dataSource = dataSource;
 		this.packagePath = packagePath;
 		this.xmlFileName = xmlFileName;
 	}

 	/**
 	 * @return String like "com/acme/app/model"
 	 */
 	public String getPackagePath() {
 		return packagePath;
 	}

 	/**
 	 * @return String
 	 */
 	public String getFileName() {
 		return xmlFileName;
 	}

 	/**
 	 * <p>Open resource containing the JDO XML description of a schema</p>
 	 * If XML file name was set to <b>null</b> in the constructor then metadata.xml is used as file name by default.
 	 * @return InputStream
 	 * @throws IOException
 	 * @throws FileNotFoundException
 	 */
 	public InputStream openStream() throws IOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JdoPackageMetadata.openStream()");
			DebugFile.incIdent();
		}
 		final ClassLoader cl = Thread.currentThread().getContextClassLoader();
		final String resourcePath = String.join("/", Str.dechomp(getPackagePath(),'/'), getFileName()==null ? "metadata.xml" : getFileName());
		if (DebugFile.trace) {
			DebugFile.writeln("ClassLoader..getResourceAsStream(" + resourcePath +")");
		}
		InputStream retval = cl.getResourceAsStream(resourcePath);
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JdoPackageMetadata.openStream() : " + retval);
		}
		return retval;
 	}

	/**
	 * <p>Create schema metadata from JDO XML description.</p>
	 * @param in InputStream 
	 * @return SchemaMetaData
	 * @throws JDOException
	 * @throws IOException
	 * @throws NullPointerException if InputStream is <b>null</b>
	 */
	@Override
	public SchemaMetaData readMetadata(InputStream in) throws JDOException, IOException {
		JdoXmlMetadata xmlparser = new JdoXmlMetadata(dataSource);
		SchemaMetaData metadata = xmlparser.readMetadata(in);
		return metadata;
	}
	
}
