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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Pattern;

import javax.jdo.JDOUserException;
import javax.jdo.JDOUnsupportedOptionException;

import com.knowgate.debug.DebugFile;

/**
 * In-memory structure for holding a data model
 * @author Sergio Montoro Ten
 *
 */
public class SchemaMetaData {
	
	private String catalogName;
	private String schemaName;
	private LinkedList<ClassPackage> packages;
	private LinkedHashMap<String,ViewDef> viewDefs;
	private LinkedHashMap<String,TableDef> tbleDefs;
	private LinkedHashMap<String,ProcedureDef> procDefs;
	private LinkedHashMap<String,TriggerDef> trigDefs;
	private LinkedHashMap<String,SequenceDef> seqDefs;
	private Pattern ymd;

	public SchemaMetaData() {
		schemaName = "";
		catalogName = "";
		viewDefs = new LinkedHashMap<String, ViewDef>(499);
		tbleDefs = new LinkedHashMap<String, TableDef>(499);
		procDefs = new LinkedHashMap<String, ProcedureDef>(499);
		trigDefs = new LinkedHashMap<String, TriggerDef>(499);
		seqDefs  = new LinkedHashMap<String,SequenceDef>();
		packages = new LinkedList<ClassPackage>();
		ymd = Pattern.compile("(_\\d{4}-\\d{1,2}-\\d{1,2})$");
	}

	/**
	 * Get instance of ClassPackage, ViewDef, TableDef, ProcedureDef, TriggerDef or SequenceDef. 
	 * @param objectName String object name
	 * @return ExtendableDef instance or <b>null</b> if this SchemaMetaData does not contain an object with the given name
	 */
	public ExtendableDef get(String objectName) {
		if (containsPackage(objectName))
			return getPackage(objectName);
		else if (containsTable(objectName))
			return getTable(objectName);
		else if (containsView(objectName))
			return getView(objectName);			
		else if (containsProcedure(objectName))
			return getProcedure(objectName);			
		else if (containsTrigger(objectName))
			return getTrigger(objectName);			
		else if (containsSequence(objectName))
			return getSequence(objectName);
		else
			return null;
	}

	/**
	 * Add objects from another SchemaMetaData instance
	 * @param metadata SchemaMetaData
	 * @throws JDOUserException If this SchemaMetaData already contains any object contained at given SchemaMetaData
	 */
	public void addMetadata(SchemaMetaData metadata) throws JDOUserException {
		for (ClassPackage pkg : metadata.packages)
			if (containsPackage(pkg.getName()))
				throw new JDOUserException("SchemaMetaData already contains a package named "+pkg.getName());
		for (String tableName : metadata.tbleDefs.keySet())
			if (containsTable(tableName))
				throw new JDOUserException("SchemaMetaData already contains a table named "+tableName);
		for (String viewName : metadata.viewDefs.keySet())
			if (containsTable(viewName))
				throw new JDOUserException("SchemaMetaData already contains a table named "+viewName);
		for (String procName : metadata.procDefs.keySet())
			if (containsProcedure(procName))
				throw new JDOUserException("SchemaMetaData already contains a procedure named "+procName);
		for (String trigName : metadata.trigDefs.keySet())
			if (containsProcedure(trigName))
				throw new JDOUserException("SchemaMetaData already contains a procedure named "+trigName);
		for (String seqName : metadata.seqDefs.keySet())
			if (containsProcedure(seqName))
				throw new JDOUserException("SchemaMetaData already contains a procedure named "+seqName);
		for (ClassPackage pkg : metadata.packages)
			addPackage(pkg);
		for (String tableName : metadata.tbleDefs.keySet()) {
			String packageName = null;
			for (ClassPackage pkg : metadata.packages) {
				if (pkg.containsClass(tableName)) {
					packageName = pkg.getName();
					break;
				}
			}
			addTable(metadata.getTable(tableName), packageName);
		}
		for (String viewName : metadata.viewDefs.keySet())
			addView(metadata.getView(viewName));
		for (String procName : metadata.procDefs.keySet())
			addProcedure(metadata.getProcedure(procName));
		for (String trigName : metadata.trigDefs.keySet())
			addTrigger(metadata.getTrigger(trigName));
		for (String seqName : metadata.seqDefs.keySet())
			if (containsProcedure(seqName))
				throw new JDOUserException("SchemaMetaData already contains a procedure named "+seqName);
	}

	/**
	 * Create a new ClassPackage instance and add it to this SchemaMetaData
	 * @param packageName String
	 * @return ClassPackage
	 */
	public ClassPackage addPackage(String packageName) {
		ClassPackage pckg = new ClassPackage();
		pckg.setName(packageName);
		packages.add(pckg);
		return pckg;
	}

	/**
	 * Add an existing ClassPackage instance to this SchemaMetaData
	 * @param classPackage ClassPackage
	 */
	public void addPackage(ClassPackage classPackage) {
		packages.add(classPackage);
		for (TableDef tdef : classPackage.getClasses())
			tbleDefs.put(tdef.getName(), tdef);
	}

	/**
	 * Get ClassPackage by name. Package names are case insensitive.
	 * @param packageName String ClassPackage Name
	 * @return ClassPackage or <b>null</b> if this SchemaMetaData does not contain a package with the given name
	 */
	public ClassPackage getPackage(String packageName) {
		for (ClassPackage pckg : packages())
			if (pckg.getName().equalsIgnoreCase(packageName))
				return pckg;
		return null;
	}

	/**
	 * Check whether this SchemaMetaData contains a ClassPackage with the given name. Package names are case insensitive.
	 * @param packageName String ClassPackage Name
	 * @return boolean
	 */
	public boolean containsPackage(String packageName) {
		for (ClassPackage pckg : packages())
			if (pckg.getName().equalsIgnoreCase(packageName))
				return true;
		return false;
	}
	
	/**
	 * Add an existing TableDef instance to this SchemaMetaData.
	 * If this instance already contains a TableDef with the same name as the provided then the existing TableDef is replaced.
	 * @param tableDef TableDef
	 * @param packageName String
	 */
	public void addTable(TableDef tableDef, String packageName) {
		
		if (DebugFile.trace) {
			DebugFile.writeln("Begin SchemaMetaData.addTable("+tableDef.getName()+","+packageName+")");
			DebugFile.incIdent();
			for (ClassPackage pckg : packages())
				if (pckg.getName().equalsIgnoreCase(packageName==null ? "default" : packageName)) {
					DebugFile.write("package "+pckg.getName()+" contains tables: ");
					for (TableDef t : pckg.getClasses())
						DebugFile.write(t.getName());
					DebugFile.writeln("");
			}
		}
		
		final String key = tableDef.getName().toLowerCase();

		if (tbleDefs.containsKey(key))
			tbleDefs.remove(key);
		tbleDefs.put(key, tableDef);
		if (null==packageName) {
			boolean existsDefault = false;
			ClassPackage defaultPackage = null;
			for (ClassPackage pckg : packages()) {
				existsDefault = pckg.getName().equalsIgnoreCase("default");
				if (existsDefault) {
					defaultPackage = pckg;
					break;
				}
			}
			if (existsDefault) {
				for (TableDef tdef : defaultPackage.getClasses()) {
					if (tdef.getName().toLowerCase().equals(key)) {
						if (DebugFile.trace) DebugFile.decIdent();
						throw new IllegalArgumentException("Package "+defaultPackage.getName()+" already contains a definition for table "+tableDef.getName());					
					}
				}
				defaultPackage.addClass(tableDef);
			} else {
				defaultPackage = new ClassPackage("default");
				packages.add(defaultPackage);
				defaultPackage.addClass(tableDef);
			}
		} else {
			for (ClassPackage pckg : packages())
				if (pckg.getName().equalsIgnoreCase(packageName)) {
					for (TableDef tdef : pckg.getClasses()) {
						if (tdef.getName().toLowerCase().equals(key)) {
							if (DebugFile.trace) DebugFile.decIdent();
							throw new IllegalArgumentException("Package "+packageName+" already contains a definition for table "+tableDef.getName());
						}
					}
					pckg.addClass(tableDef);
					break;
				}			
		}
		
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End SchemaMetaData.addTable()");
		}
	}

	/**
	 * Remove from this SchemaMetadata the reference to TableDef with the given name.
	 * If this SchemaMetadata does not contain a TableDef with the given name then no exception is thrown.
	 * @param tableName String
	 */
	public void removeTable(String tableName, String packageName) {
		final String key = tableName.toLowerCase();
		if (tbleDefs.containsKey(key))
			tbleDefs.remove(key);
		if (null==packageName) {
			for (ClassPackage pckg : packages()) {
				if (pckg.getName().equalsIgnoreCase("default")) {
					for (TableDef tdef : pckg.getClasses())
						if (tdef.getName().toLowerCase().equals(key)) {
							pckg.removeClass(key);
							break;
						}
					break;
				}
			}
		} else {
			for (ClassPackage pckg : packages())
				if (pckg.getName().equalsIgnoreCase(packageName)) {
					boolean found = false;
					for (TableDef tdef : pckg.getClasses())
						if (tdef.getName().toLowerCase().equals(key)) {
							pckg.removeClass(key);
							found = true;
							break;
						}
					if (!found)
						throw new IllegalArgumentException("Package "+packageName+" does not contain a definition for table "+tableName);
					break;
				}			
		}
	}

	/**
	 * @param tableName String Table Name
	 * @return TableDef or <b>null</b> if this SchemaMetadata does not contain a TableDef with the given name.
	 */
	public TableDef getTable(String tableName) {
		return tbleDefs.get(tableName.toLowerCase());
	}

	/**
	 * Check whether this SchemaMetaData contains a TableDef with the given name. Table names are case insensitive.
	 * @param tableName String TableDef Name
	 * @return boolean
	 */
	public boolean containsTable(String tableName) {
		return tbleDefs.containsKey(tableName.toLowerCase());
	}

	/**
	 * Add an existing ViewDef instance to this SchemaMetaData.
	 * If this instance already contains a ViewDef with the same name as the provided then the existing ViewDef is replaced.
	 * @param viewDef ViewDef
	 */
	public void addView(ViewDef viewDef) {
		final String key = viewDef.getName().toLowerCase();
		if (viewDefs.containsKey(key))
			viewDefs.remove(key);
		viewDefs.put(key, viewDef);
	}

	/**
	 * Remove from this SchemaMetadata the reference to ViewDef with the given name.
	 * If this SchemaMetadata does not contain a ViewDef with the given name then no exception is thrown.
	 * @param viewName String
	 */
	public void removeView(String viewName) {
		final String key = viewName.toLowerCase();
		if (viewDefs.containsKey(key))
			viewDefs.remove(key);
	}

	/**
	 * @param viewName String View Name
	 * @return ViewDef or <b>null</b> if this SchemaMetadata does not contain a ViewDef with the given name.
	 */
	public ViewDef getView(String viewName) {
		return viewDefs.get(viewName.toLowerCase());
	}

	/**
	 * Check whether this SchemaMetaData contains a ViewDef with the given name. View names are case insensitive.
	 * @param viewName String ViewDef Name
	 * @return boolean
	 */
	public boolean containsView(String viewName) {
		return viewDefs.containsKey(viewName.toLowerCase());
	}

	/**
	 * Add an existing UniqueIndexDef or NonUniqueIndexDef instance to this SchemaMetaData.
	 * If this instance already contains a IndexDef with the same name as the provided then the existing IndexDef is replaced.
	 * @param indexDef IndexDef
	 * @throws JDOUserException If the table referenced by the IndexDef is  not contained in this SchemaMetadata.
	 * @throws JDOUnsupportedOptionException
	 */
	public void addIndex(IndexDef indexDef) throws JDOUserException, JDOUnsupportedOptionException {
		TableDef tdef = getTable(indexDef.getTable());
		if (null==tdef)
			throw new JDOUserException("Table "+indexDef.getTable()+" for index "+indexDef.getName());
		if (indexDef instanceof UniqueIndexDef)
			tdef.addUniqueMetadata((UniqueIndexDef) indexDef);
		else if (indexDef instanceof NonUniqueIndexDef)
			tdef.addIndexMetadata((NonUniqueIndexDef) indexDef);
		else
			throw new JDOUnsupportedOptionException("Unsupported index type "+indexDef.getClass().getName());
	}

	/**
	 * Add an existing ProcedureDef instance to this SchemaMetaData.
	 * If this instance already contains a ProcedureDef with the same name as the provided then the existing ProcedureDef is replaced.
	 * @param procDef ProcedureDef
	 */
	public void addProcedure(ProcedureDef procDef) {
		final String key = procDef.getName().toLowerCase();
		if (procDefs.containsKey(key))
			procDefs.remove(key);
		procDefs.put(key, procDef);
	}

	/**
	 * Remove from this SchemaMetadata the reference to ProcedureDef with the given name.
	 * If this SchemaMetadata does not contain a ProcedureDef with the given name then no exception is thrown.
	 * @param procedureName String
	 */
	public void removeProcedure(String procedureName) {
		final String key = procedureName.toLowerCase();
		if (procDefs.containsKey(key))
			procDefs.remove(key);
	}

	/**
	 * @param procedureName String Procedure Name
	 * @return ProcedureDef or <b>null</b> if this SchemaMetadata does not contain a ProcedureDef with the given name.
	 */
	public ProcedureDef getProcedure(String procedureName) {
		return procDefs.get(procedureName.toLowerCase());
	}
	
	/**
	 * Check whether this SchemaMetaData contains a ProcedureDef with the given name. Procedure names are case insensitive.
	 * @param procedureName String ProcedureDef Name
	 * @return boolean
	 */
	public boolean containsProcedure(String procedureName) {
		return procDefs.containsKey(procedureName);
	}

	public void addTrigger(TriggerDef trigDef) {
		final String key = trigDef.getName().toLowerCase();
		if (trigDefs.containsKey(key))
			trigDefs.remove(key);
		trigDefs.put(key, trigDef);
	}

	public void removeTrigger(String triggerName) {
		final String key = triggerName.toLowerCase();
		if (trigDefs.containsKey(key))
			trigDefs.remove(key);
	}

	public TriggerDef getTrigger(String triggerName) {
		return trigDefs.get(triggerName.toLowerCase());
	}
	
	public boolean containsTrigger(String triggerName) {
		return trigDefs.containsKey(triggerName);
	}

	public void addSequence(SequenceDef seqDef) {
		final String key = seqDef.getName().toLowerCase();
		if (seqDefs.containsKey(key))
			seqDefs.remove(key);
		seqDefs.put(key, seqDef);
	}

	public void removeSequence(String sequenceName) {
		final String key = sequenceName.toLowerCase();
		if (seqDefs.containsKey(key))
			seqDefs.remove(key);
	}

	public SequenceDef getSequence(String sequenceName) {
		return seqDefs.get(sequenceName.toLowerCase());
	}
	
	public boolean containsSequence(String sequenceName) {
		return seqDefs.containsKey(sequenceName);
	}
	
	public void clear() {
		tbleDefs.clear();
		viewDefs.clear();
		procDefs.clear();
		trigDefs.clear();
		seqDefs.clear();
		packages.clear();
	}

	public Collection<ClassPackage> packages() {
		return Collections.unmodifiableCollection(packages);
	}
	
	public Collection<TableDef> tables() {
		return Collections.unmodifiableCollection(tbleDefs.values());
	}

	public Collection<ViewDef> views() {
		return Collections.unmodifiableCollection(viewDefs.values());
	}
	
	public Collection<IndexDef> indexes() {
		LinkedList<IndexDef> allIndexes = new LinkedList<IndexDef>();
		for (TableDef tdef : tables())
			for (IndexDef idx : tdef.getIndices())
				allIndexes.add(idx);
		return Collections.unmodifiableCollection(allIndexes);
	}

	public Collection<ProcedureDef> procedures() {
		return Collections.unmodifiableCollection(procDefs.values());
	}

	public Collection<TriggerDef> triggers() {
		return Collections.unmodifiableCollection(trigDefs.values());
	}

	public Collection<SequenceDef> sequences() {
		return Collections.unmodifiableCollection(seqDefs.values());
	}

	public int getTablesCount() {
		return tbleDefs.size();
	}
	
	public Map<String,TableDef> tableMap() {
		return Collections.unmodifiableMap(tbleDefs);
	}
	
	public String getCatalog() {
		return catalogName;
	}

	public void setCatalog(String name) {
		catalogName = name;
	}
	
	public String getSchema() {
		return schemaName;
	}

	public void setSchema(String name) {
		schemaName = name;
	}
	
	/**
	 * Get columns of a given table.
	 * @param tableName String
	 * @return ColumnDef[]
	 * @throws ArrayIndexOutOfBoundsException If no table with the given name is contained at this SchemaMetadata
	 */
	public ColumnDef[] getColumns(String tableName)
		throws ArrayIndexOutOfBoundsException {
		final String key = tableName.toLowerCase();
		boolean matches = ymd.matcher(tableName).find();
		if (DebugFile.trace)
			DebugFile.writeln(tableName+" matches "+ymd.toString()+" "+String.valueOf(matches));
		if (matches)
			if (DebugFile.trace)
				DebugFile.writeln("Catalog contains key "+tableName.substring(0, tableName.length()-ymd.toString().length())+" "+tbleDefs.containsKey(key.substring(0, key.length()-ymd.toString().length())));
		if (tbleDefs.containsKey(tableName))
			return tbleDefs.get(key).getColumns();
		else {
			boolean bTimeRollingTable = ymd.matcher(tableName).find();
			if (bTimeRollingTable) {
			  String baseTable = key.substring(0, key.lastIndexOf('_'));
			  if (tbleDefs.containsKey(baseTable))
		        return tbleDefs.get(baseTable).getColumns();
		      else
		        throw new ArrayIndexOutOfBoundsException("No base Table found with name "+baseTable);
			} else {
		        throw new ArrayIndexOutOfBoundsException("No Table nor Record found with name "+tableName);			
			}
		}	
	}

	/**
	 * Get name of columns of a given table
	 * @param tableName String
	 * @return String[]
	 * @throws ArrayIndexOutOfBoundsException If no table with the given name is contained at this SchemaMetadata
	 */
	public String[] getColumnNames(String tableName)
			throws ArrayIndexOutOfBoundsException {
		String[] columnNames;
		ColumnDef[] columnsList = getColumns(tableName);
		if (ymd.matcher(tableName).find())
			tableName = tableName.substring(0, tableName.lastIndexOf('_'));
		if (!tbleDefs.containsKey(tableName.toLowerCase())) {
		  columnNames = new String[columnsList.length];
		  for (int nCol=0; nCol<columnsList.length; nCol++)
		    columnNames[nCol] = columnsList[nCol].getName();
		} else {
			columnNames = new String[0];
		}
		return columnNames;
	}

}
