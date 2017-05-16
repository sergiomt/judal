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
import java.util.LinkedHashMap;
import java.util.Collections;

/**
 * JDO metadata package definition
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class ClassPackage extends ExtendableDef {

	private static final long serialVersionUID = 10000l;

	LinkedHashMap<String, TableDef> classes;

	private String name; 
	
	public ClassPackage() {
		classes = new LinkedHashMap<String, TableDef>();
	}

	public ClassPackage(String packageName) {
		classes = new LinkedHashMap<String, TableDef>();
		name = packageName;
	}

	/**
	 * @return String Package name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param String Package name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * <p>Add TableDef to this package.</p>
	 * @param TableDef
	 */
	public void addClass(TableDef tableDef) {
		classes.put(tableDef.getName().toLowerCase(), tableDef);
	}
	
	/**
	 * <p>Remove TableDef from this package.</p>
	 * @param String
	 */
	public void removeClass(String className) {
		classes.remove(className.toLowerCase());
	}

	/**
	 * @param String
	 * @return boolean
	 */
	public boolean containsClass(String className) {
		return classes.containsKey(className.toLowerCase());
	}
	
	/**
	 * @return Unmodifiable collection of TableDef instances
	 */
	public Collection<TableDef> getClasses() {
		return Collections.unmodifiableCollection(classes.values());
	}

	/**
	 * @return String like &lt;package name="package_name"&gt;<i>TableDefXml</i>\n<i>TableDefXml</i>&hellip;&lt;/package&gt;\n
	 */
	public String toJdoXml() {
		StringBuilder builder = new StringBuilder();
		builder.append("    <package name=\""+getName()+"\">\n");
		for (TableDef tdef : classes.values())
		    builder.append(tdef.toJdoXml()).append("\n");
		builder.append("    </package>\n");
		return builder.toString();
	}
}
