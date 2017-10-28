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

import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;

import org.judal.jdbc.JDBCTableDataSource;
import org.judal.metadata.ProcedureDef;
import org.judal.metadata.SchemaManager;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.Scriptable;
import org.judal.metadata.SequenceDef;
import org.judal.metadata.TableDef;
import org.judal.metadata.TriggerDef;
import org.judal.metadata.ViewDef;

import com.knowgate.debug.DebugFile;

public class SQLSchemaManager implements SchemaManager {

	private boolean stopOnError;
	private PrintWriter printer;
	private JDBCTableDataSource dataSource;

	public SQLSchemaManager(JDBCTableDataSource dataSource, PrintWriter printer) {
		this.dataSource = dataSource;
		this.printer = printer;
		this.stopOnError = false;
	}

	private int doDdl(Set<String> done, String objectName, String ddl) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin SQLSchemaManager.doDdl("+done+","+objectName+")");
			DebugFile.incIdent();
			DebugFile.writeln(ddl);
		}
		int errorCount = 0;
		done.add(objectName);
		if (stopOnError) {
			dataSource.execute(ddl);				
		} else {
			try {
				dataSource.execute(ddl);
			} catch (Exception xcpt)  {
				errorCount++;
				printer.println(xcpt.getClass().getName()+" "+xcpt.getMessage());
				printer.println(ddl);
			}				
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("Begin SQLSchemaManager.doDdl() + "+String.valueOf(errorCount));
		}
		return errorCount;
	}

	private int createCascade(SQLSchemaMetaData sqlmeta, Set<String> created, String objectName) throws JDOException {
		int errorCount = 0;
		for (String dependencyName : sqlmeta.getDependencies(objectName))
			errorCount += createCascade(sqlmeta, created, dependencyName);
		errorCount += doDdl(created, objectName, ((Scriptable) sqlmeta.get(objectName)).getSource());
		return errorCount;
	}

	private int dropCascade(SQLSchemaMetaData sqlmeta, Set<String> dropped, String objectName) throws JDOException {
		int errorCount = 0;
		for (String dependencyName : sqlmeta.getDependencies(objectName))
			errorCount += createCascade(sqlmeta, dropped, dependencyName);
		errorCount += doDdl(dropped, objectName, ((Scriptable) sqlmeta.get(objectName)).getDrop());
		return errorCount;
	}
	
	@Override
	public int create(SchemaMetaData metadata) throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin SQLSchemaManager.readMetaData()");
			DebugFile.incIdent();
		}
		int errorCount = 0;
		SQLSchemaMetaData sqlmeta = (SQLSchemaMetaData) metadata;
		Set<String> created = new HashSet<String>();
		for (SequenceDef gdef : metadata.sequences())
			errorCount += doDdl(created, gdef.getName(), gdef.toString());

		if (DebugFile.trace)
			DebugFile.writeln(String.valueOf(metadata.tables().size())+" tables to create");
		for (TableDef tdef : metadata.tables())
			if (!created.contains(tdef.getName()))
				errorCount += createCascade(sqlmeta, created, tdef.getName());

		for (ViewDef vdef : metadata.views())
			if (!created.contains(vdef.getName()))
				errorCount += createCascade(sqlmeta, created, vdef.getName());

		for (ProcedureDef pdef : metadata.procedures())
			if (!created.contains(pdef.getName()))
				errorCount += createCascade(sqlmeta, created, pdef.getName());

		for (TriggerDef gdef : metadata.triggers())
			if (!created.contains(gdef.getName()))
				errorCount += createCascade(sqlmeta, created, gdef.getName());
		
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("Begin SQLSchemaManager.readMetaData() " + String.valueOf(errorCount));
		}
		return errorCount;
	}

	@Override
	public int update(SchemaMetaData metadata) throws JDOException {
		throw new JDOUnsupportedOptionException("SQLSchemaManager.update() is not implemented");
	}

	@Override
	public int drop(SchemaMetaData metadata) throws JDOException {
		int errorCount = 0;
		SQLSchemaMetaData sqlmeta = (SQLSchemaMetaData) metadata;
		Set<String> dropped = new HashSet<String>();

		for (Scriptable gdef : reverse(metadata.triggers()))
			if (!dropped.contains(gdef.getName()))
				errorCount += dropCascade(sqlmeta, dropped, gdef.getName());

		for (Scriptable pdef : reverse(metadata.procedures()))
			if (!dropped.contains(pdef.getName()))
				errorCount += dropCascade(sqlmeta, dropped, pdef.getName());

		for (Scriptable vdef : reverse(metadata.views()))
			if (!dropped.contains(vdef.getName()))
				errorCount += dropCascade(sqlmeta, dropped, vdef.getName());

		for (Scriptable tdef : reverse(metadata.tables()))
			if (!dropped.contains(tdef.getName()))
				errorCount += dropCascade(sqlmeta, dropped, tdef.getName());

		for (Scriptable gdef : reverse(metadata.sequences()))
			if (!dropped.contains(gdef.getName()))
				errorCount += dropCascade(sqlmeta, dropped, gdef.getName());
		
		return errorCount;
	}

	@Override
	public void setLogWriter(PrintWriter printer) {
		this.printer = printer;	
	}

	@Override
	public boolean stopOnError() {
		return stopOnError;
	}

	@Override
	public void stopOnError(boolean stopOnError) {
		this.stopOnError = stopOnError;
	}

	private Collection<Scriptable> reverse(Collection<?> c) {
		LinkedList<Scriptable> r  = new LinkedList<Scriptable>();
		for (Object s : c)
			r.addFirst((Scriptable) s);
		return r;
	}

}
