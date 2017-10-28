package org.judal.jdbc.metadata;

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
import java.io.ByteArrayOutputStream;

import java.net.URL;

import java.sql.SQLException;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Collections;

import javax.jdo.JDOException;

import org.judal.jdbc.RDBMS;
import org.judal.jdbc.JDBCTableDataSource;
import org.judal.metadata.MetadataScanner;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.bind.JdoXmlMetadata;

import com.knowgate.debug.DebugFile;
import com.knowgate.io.StreamPipe;
import com.pureperfect.ferret.ScanFilter;
import com.pureperfect.ferret.Scanner;
import com.pureperfect.ferret.vfs.PathElement;

public class SQLSchemaMetaData extends SchemaMetaData {

	private ClassLoader clssLdr;
	private JDBCTableDataSource dataSource;
	private String packagePath, fileName;
	private Map<String,Set<String>> requirements;
	private static final Set<String> EMPTY_SET = Collections.unmodifiableSet(new TreeSet<String>());

	protected boolean isFile(PathElement resource) {

		String resClass = resource.getClass().getName();
		return resClass.equals("com.pureperfect.ferret.vfs.ImplFileFile") || resClass.equals("com.pureperfect.ferret.vfs.ImplArchiveEntry");
	}

	protected boolean hasExtension(PathElement resource, String... exts) {
		boolean hasIt = false;
		for (String ext : exts)
			hasIt = hasIt || resource.getFullPath().toLowerCase().endsWith(ext);
		return hasIt;
	}
	
	protected final ScanFilter all = new ScanFilter() {
		@Override
		public boolean accept(final PathElement resource) {
			return true;
		}
 	};

 	protected final ScanFilter sql = new ScanFilter() {
		@Override
		public boolean accept(final PathElement resource) {
			return isFile(resource) && hasExtension(resource, ".sql", ".ddl");
		}
 	};

	protected final ScanFilter xml = new ScanFilter() {
		@Override
		public boolean accept(final PathElement resource) {
			return isFile(resource) && hasExtension(resource, ".xml");
		}
 	};
 	
	public SQLSchemaMetaData(JDBCTableDataSource dataSource, String packagePath, String fileName)
		throws JDOException, NoSuchMethodException, IOException {
		this.packagePath = packagePath;
		this.fileName = fileName;
		this.dataSource = dataSource;
		this.requirements = new HashMap<String,Set<String>>();
		clssLdr = Thread.currentThread().getContextClassLoader();
		readMetadata();
	}

	private Set<? extends PathElement> getElements(String path, ScanFilter filter) throws IOException {
		Scanner scn = new Scanner();
		Enumeration<URL> urls = clssLdr.getResources(path);
		while (urls.hasMoreElements())
			scn.add(urls.nextElement());
		return scn.scan(filter);
	}

	private Set<String> scanRequirements(String fileName, String fileContents) {
		String[] lines = fileContents.split("\n");
		TreeSet<String> dependencies = new TreeSet<String>();
		Pattern require = Pattern.compile("( *-- *require: *).");
		if (lines.length>0) {
			for (String line : lines) {
				Matcher match = require.matcher(line);
				if (match.matches())
					dependencies.add(line.substring(match.group(1).length()).trim());
				else
					break;
			}		
		}
		return dependencies;
	}

	private String readElement(PathElement resource) throws IOException {
		StreamPipe pipe = new StreamPipe();
		ByteArrayOutputStream raw = new ByteArrayOutputStream(8000);
		InputStream instrm = resource.openStream();
		pipe.between(instrm, raw);
		String contents = raw.toString("UTF8");
		instrm.close();
		raw.close();
		return contents;
	}

	private MetadataScanner getScanner(PathElement resource) throws IOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin SQLSchemaMetaData.getScanner(" + resource.getName() + ")");
			DebugFile.incIdent();
		}
		MetadataScanner retval = null;
		String fileContents = readElement(resource);
		String[] lines = fileContents.split("\n");
		final int count = lines.length<10 ? lines.length : 20;
		for (int l=0; l<count; l++)
			if (lines[l].indexOf("xmlns=\"http://db.apache.org/ddlutils/schema/1.")>0)
				retval = new SQLXmlMetadata(dataSource);
			else if (lines[l].indexOf("xmlns=\"http://java.sun.com/xml/ns/jdo/jdo\"")>0)
				retval = new JdoXmlMetadata(dataSource);
		if (DebugFile.trace)
			DebugFile.decIdent();
		if (null==retval)
			throw new IOException("Could not find any class suitable to parse "+resource.getFullPath());
		if (DebugFile.trace) {
			DebugFile.writeln("End SQLSchemaMetaData.getScanner() : " + retval.getClass().getName());
		}
		return retval;
	}

	public Set<String> getDependencies(String objectName) {
		if (requirements.containsKey(objectName))
			return requirements.get(objectName);
		else
			return EMPTY_SET;
	}

	public void readMetadata() throws JDOException, IOException, NoSuchMethodException {
		RDBMS dbms;
		if (DebugFile.trace) {
			DebugFile.writeln("Begin SQLSchemaMetaData.readMetaData()");
			DebugFile.incIdent();
		}
		try {
			dbms = dataSource.getDatabaseProductId();
		} catch (SQLException sqle) {
			throw new JDOException(sqle.getMessage(), sqle);
		}
		Set<? extends PathElement> resources = getElements(packagePath+"/"+fileName, all);
		if (resources.isEmpty())
			throw new java.io.FileNotFoundException("Resource not found "+packagePath+"/"+fileName);
		PathElement resource = resources.iterator().next();
		MetadataScanner xmlScanner = getScanner(resource);
		if (DebugFile.trace)
			DebugFile.writeln("open stream " + resource.getFullPath());
		InputStream instrm = resource.openStream();
		addMetadata(xmlScanner.readMetadata(instrm));
		instrm.close();
		SQLDdlMetadata sqlScanner = new SQLDdlMetadata(dbms);
		for (String resType : new String[]{"sequences","views","procedures","triggers"}) {
			resources = getElements(packagePath+"/"+resType+"/"+dbms.shortName(), sql);
			if (resources!=null) {
				for (PathElement res : resources) {
					String fileName = res.getName();
					String fileContents = readElement(res);
					final int dot = fileName.lastIndexOf('.');
					final String objectName = dot>0 ? fileName.substring(0,dot) : fileName;
					requirements.put(objectName, scanRequirements(fileName, fileContents));
					addMetadata(sqlScanner.readMetadata(fileContents));
				}
			}	
		}	
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End SQLSchemaMetaData.readMetaData()");
		}
	}

}
