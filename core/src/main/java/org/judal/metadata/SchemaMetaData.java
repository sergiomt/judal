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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Pattern;

import javax.jdo.JDOUserException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.metadata.ColumnMetadata;

/**
 * In-memory structure for holding a data model
 * @author Sergio Montoro Ten
 *
 */
public class SchemaMetaData {

	private String catalogName;
	private String schemaName;
	private final LinkedList<ClassPackage> packages;
	private final LinkedHashMap<String,ViewDef> viewDefs;
	private final LinkedHashMap<String,TableDef> tbleDefs;
	private final LinkedHashMap<String,ProcedureDef> procDefs;
	private final LinkedHashMap<String,TriggerDef> trigDefs;
	private final LinkedHashMap<String,SequenceDef> seqDefs;
	private Pattern ymd;

	public SchemaMetaData() {
		schemaName = "";
		catalogName = "";
		viewDefs = new LinkedHashMap<>(499);
		tbleDefs = new LinkedHashMap<>(499);
		procDefs = new LinkedHashMap<>(499);
		trigDefs = new LinkedHashMap<>(499);
		seqDefs  = new LinkedHashMap<>();
		packages = new LinkedList<>();
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
				throw new JDOUserException("SchemaMetaData already contains a package named " + pkg.getName());
		for (String tableName : metadata.tbleDefs.keySet())
			if (containsTable(tableName))
				throw new JDOUserException("SchemaMetaData already contains a table named " + tableName);
		for (String viewName : metadata.viewDefs.keySet())
			if (containsView(viewName))
				throw new JDOUserException("SchemaMetaData already contains a view named " + viewName);
		for (String procName : metadata.procDefs.keySet())
			if (containsProcedure(procName))
				throw new JDOUserException("SchemaMetaData already contains a procedure named " + procName);
		for (String trigName : metadata.trigDefs.keySet())
			if (containsTrigger(trigName))
				throw new JDOUserException("SchemaMetaData already contains a trigger named " + trigName);
		for (String seqName : metadata.seqDefs.keySet())
			if (containsSequence(seqName))
				throw new JDOUserException("SchemaMetaData already contains a sequence named " + seqName);

		for (ClassPackage pkg : metadata.packages)
			addPackage(pkg.getName());

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
			addSequence(metadata.getSequence(seqName));

	}

	/**
	 * Create a new ClassPackage instance and add it to this SchemaMetaData
	 * @param packageName String
	 * @return ClassPackage
	 * @throws JDOUserException if this SchemaMetadata already contains a package with same name as packageName
	 */
	public ClassPackage addPackage(String packageName) {

		if (containsPackage(packageName))
			throw new JDOUserException("SchemaMetaData already contains a package named " + packageName);

		ClassPackage pckg = new ClassPackage();
		pckg.setName(packageName);
		packages.add(pckg);
		return pckg;
	}

	/**
	 * Add an existing ClassPackage instance to this SchemaMetaData
	 * @param classPackage ClassPackage
	 * @throws JDOUserException if this SchemaMetadata already
	 * contains a package with same name or a table with same name
	 * as any table contained in the package being added
	 */
	public void addPackage(ClassPackage classPackage) throws JDOUserException {

		if (containsPackage(classPackage.getName()))
			throw new JDOUserException("SchemaMetaData already contains a package named " + classPackage.getName());
		for (TableDef tdef : classPackage.getClasses())
			if (tbleDefs.containsKey(tdef.getName()))
				throw new JDOUserException("SchemaMetaData already contains a table named " + tdef.getName());

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
							throw new IllegalArgumentException("Package "+packageName+" already contains a definition for table "+tableDef.getName());
						}
					}
					pckg.addClass(tableDef);
					break;
				}			
		}

	}

	/**
	 * Get a Collection with all the objects contained in all the packages of this SchemaMetaData.
	 * The objects are returned in groups sequences, tables, views, procedures and last triggers.
	 * @return Collection&lt;ExtendableDef&gt;
	 */
	public Collection<ExtendableDef> all() {
		ArrayList<ExtendableDef> allObjects = new ArrayList<ExtendableDef>(seqDefs.size()+trigDefs.size()+procDefs.size()+tbleDefs.size()+viewDefs.size());
		allObjects.addAll(seqDefs.values());
		allObjects.addAll(tbleDefs.values());
		allObjects.addAll(viewDefs.values());
		allObjects.addAll(procDefs.values());
		allObjects.addAll(trigDefs.values());
		return Collections.unmodifiableCollection(allObjects);
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
	 * <p>Get TableDef for a table with a given name.</p>
	 * If the provided table name ends with a string date pattern like _YYYY-MM-DD then a corresponding table without that suffix will also be sought
	 * @param tableName String Table Name
	 * @return TableDef or <b>null</b> if this SchemaMetadata does not contain a TableDef with the given name.
	 */
	public TableDef getTable(String tableName) {
		if (null==tableName) return null;
		TableDef tdef = tbleDefs.get(tableName.toLowerCase());
		if (null==tdef && ymd.matcher(tableName).matches())
			tdef = tbleDefs.get(tableName.substring(0,tableName.lastIndexOf('_')).toLowerCase());
		return tdef;
	}

	/**
	 * <p>Check whether this SchemaMetaData contains a TableDef with the given name. Table names are case insensitive.</p>
	 * If the provided table name ends with a string date pattern like _YYYY-MM-DD then a corresponding table without that suffix will also be sought
	 * @param tableName String TableDef Name
	 * @return boolean
	 */
	public boolean containsTable(String tableName) {
		if (null==tableName) return false;
		boolean bfound = tbleDefs.containsKey(tableName.toLowerCase());
		if (!bfound && ymd.matcher(tableName).matches())
			bfound = tbleDefs.containsKey(tableName.substring(0,tableName.lastIndexOf('_')).toLowerCase());
		return bfound;
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
	 * @throws ArrayIndexOutOfBoundsException if any of the index columns does not exist on the indexed table
	 */
	public void addIndex(IndexDef indexDef) throws JDOUserException, JDOUnsupportedOptionException, ArrayIndexOutOfBoundsException {
		TableDef tdef = getTable(indexDef.getTable());
		if (null==tdef)
			throw new JDOUserException("Table "+indexDef.getTable()+" for index "+indexDef.getName());
		if (indexDef instanceof UniqueIndexDef)
			tdef.addUniqueMetadata((UniqueIndexDef) indexDef);
		else if (indexDef instanceof NonUniqueIndexDef)
			tdef.addIndexMetadata((NonUniqueIndexDef) indexDef);
		else
			throw new JDOUnsupportedOptionException("Unsupported index type "+indexDef.getClass().getName());
		for (ColumnMetadata cmeta : indexDef.getColumns()) {
			ColumnDef cdef = tdef.getColumnByName(cmeta.getName());
			if (!cdef.isIndexed() && !cdef.isPrimaryKey())
				cdef.setIndexType(indexDef.getType());
		}
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

	/**
	 * Add trigger.
	 * @param trigDef TriggerDef
	 */
	public void addTrigger(TriggerDef trigDef) {
		final String key = trigDef.getName().toLowerCase();
		if (trigDefs.containsKey(key))
			trigDefs.remove(key);
		trigDefs.put(key, trigDef);
	}

	/**
	 * Remove trigger. I no trigger is found with the given name this method does nothing and does not return any error.
	 * @param triggerName String
	 */
	public void removeTrigger(String triggerName) {
		final String key = triggerName.toLowerCase();
		if (trigDefs.containsKey(key))
			trigDefs.remove(key);
	}

	/**
	 * Get TriggerDef by name. Lookup is case insensitive.
	 * @param triggerName String
	 * @return TriggerDef or <b>nulll</b> if no trigger is found with given name
	 */
	public TriggerDef getTrigger(String triggerName) {
		return trigDefs.get(triggerName.toLowerCase());
	}

	/**
	 * Get whether this SchemaMetadata contains a TriggerDef by the given name. Name lookup is case insensitive.
	 * @param triggerName String
	 * @return boolean
	 */
	public boolean containsTrigger(String triggerName) {
		return trigDefs.containsKey(triggerName);
	}

	/**
	 * Add or replace Sequence
	 * @param seqDef SequenceDef
	 */
	public void addSequence(SequenceDef seqDef) {
		final String key = seqDef.getName().toLowerCase();
		if (seqDefs.containsKey(key))
			seqDefs.remove(key);
		seqDefs.put(key, seqDef);
	}


	/**
	 * Remove sequence. I no sequence is found with the given name this method does nothing and does not return any error.
	 * @param sequenceName String
	 */
	public void removeSequence(String sequenceName) {
		final String key = sequenceName.toLowerCase();
		if (seqDefs.containsKey(key))
			seqDefs.remove(key);
	}

	/**
	 * Get SequenceDef by name. Lookup is case insensitive.
	 * @param sequenceName String
	 * @return SequenceDef or <b>nulll</b> if no sequence is found with given name
	 */
	public SequenceDef getSequence(String sequenceName) {
		return seqDefs.get(sequenceName.toLowerCase());
	}

	/**
	 * Get whether this SchemaMetadata contains a SequenceDef by the given name. Name lookup is case insensitive.
	 * @param sequenceName String
	 * @return boolean
	 */
	public boolean containsSequence(String sequenceName) {
		return seqDefs.containsKey(sequenceName);
	}

	/**
	 * Remove all tables,views, procedures, triggers, sequences and packages
	 */
	public void clear() {
		tbleDefs.clear();
		viewDefs.clear();
		procDefs.clear();
		trigDefs.clear();
		seqDefs.clear();
		packages.clear();
	}

	/**
	 * Get unmodifiable collection of packages in this schema
	 * @return Collection&lt;ClassPackage&gt;
	 */
	public Collection<ClassPackage> packages() {
		return Collections.unmodifiableCollection(packages);
	}

	/**
	 * Get unmodifiable collection of tables in this schema
	 * @return Collection&lt;TableDef&gt;
	 */
	public Collection<TableDef> tables() {
		return Collections.unmodifiableCollection(tbleDefs.values());
	}

	/**
	 * Get unmodifiable collection of views in this schema
	 * @return Collection&lt;ViewDef&gt;
	 */
	public Collection<ViewDef> views() {
		return Collections.unmodifiableCollection(viewDefs.values());
	}

	/**
	 * Get unmodifiable collection of indexes in this schema
	 * @return Collection&lt;IndexDef&gt;
	 */
	public Collection<IndexDef> indexes() {
		LinkedList<IndexDef> allIndexes = new LinkedList<IndexDef>();
		for (TableDef tdef : tables())
			for (IndexDef idx : tdef.getIndices())
				allIndexes.add(idx);
		return Collections.unmodifiableCollection(allIndexes);
	}

	/**
	 * Get unmodifiable collection of procedures in this schema
	 * @return Collection&lt;ProcedureDef&gt;
	 */
	public Collection<ProcedureDef> procedures() {
		return Collections.unmodifiableCollection(procDefs.values());
	}

	/**
	 * Get unmodifiable collection of triggers in this schema
	 * @return Collection&lt;TriggerDef&gt;
	 */
	public Collection<TriggerDef> triggers() {
		return Collections.unmodifiableCollection(trigDefs.values());
	}

	/**
	 * Get unmodifiable collection of sequences in this schema
	 * @return Collection&lt;SequenceDef&gt;
	 */
	public Collection<SequenceDef> sequences() {
		return Collections.unmodifiableCollection(seqDefs.values());
	}

	/**
	 * @return int
	 */
	public int getTablesCount() {
		return tbleDefs.size();
	}

	/**
	 * Get unmodifiable map of tables in this schema
	 * @return Map&lt;String,TableDef&gt;
	 */
	public Map<String,TableDef> tableMap() {
		return Collections.unmodifiableMap(tbleDefs);
	}

	/**
	 * @return String catalogName
	 */
	public String getCatalog() {
		return catalogName;
	}

	/**
	 * @param name String
	 */
	public void setCatalog(String name) {
		catalogName = name;
	}

	/**
	 * @return String schemaName
	 */
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
