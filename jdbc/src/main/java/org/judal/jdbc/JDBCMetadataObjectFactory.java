package org.judal.jdbc;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.jdo.JDOException;

import org.judal.jdbc.metadata.SQLIndex;
import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.jdbc.metadata.SQLViewDef;
import org.judal.metadata.IndexDef;
import org.judal.metadata.MetadataObjectFactory;
import org.judal.metadata.ProcedureDef;
import org.judal.metadata.SequenceDef;
import org.judal.metadata.TableDef;
import org.judal.metadata.TriggerDef;
import org.judal.metadata.ViewDef;

/**
 * <p>Factory class for JDBC objects.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JDBCMetadataObjectFactory extends MetadataObjectFactory {

	/**
	 * <p>Create and instance of a ViewDef subclass for an specific relational database management system.</p>
	 * @param dbms RDBMS
	 * @param constructorParameters Object&hellip; Parameters for the ViewDef subclass constructor
	 * @return &lt;? extends ViewDef&gt; The returned type will be SQLViewDef unless another subclass of ViewDef has been defined for the RDBMS
	 * @throws NoSuchMethodException If no suitable constructor can be found for the given parameters
	 * @throws JDOException
	 */
	@SuppressWarnings("unchecked")
	public static <T extends ViewDef> T newViewDef(RDBMS dbms, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		Set<Class<?>> subclasses = getObjectDefSubclasses(dbms, ViewDef.class);
		Object[] viewConstrutorParameters = new Object[constructorParameters.length+1];
		viewConstrutorParameters[0] = dbms;
		for (int  p=0; p<constructorParameters.length; p++)
			viewConstrutorParameters[p+1] = constructorParameters[p];
		return newViewDef((Class<T>) (subclasses.isEmpty() ? SQLViewDef.class : subclasses.iterator().next()), viewConstrutorParameters);
	}

	/**
	 * <p>Create and instance of a TableDef subclass for an specific relational database management system.</p>
	 * @param dbms RDBMS
	 * @param constructorParameters Object&hellip; Parameters for the TableDef subclass constructor
	 * @return &lt;? extends TableDef&gt; The returned type will be SQLTableDef unless another subclass of ViewDef has been defined for the RDBMS
	 * @throws NoSuchMethodException If no suitable constructor can be found for the given parameters
	 * @throws JDOException
	 */
	@SuppressWarnings("unchecked")
	public static <T extends TableDef> T newTableDef(RDBMS dbms, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		Set<Class<?>> subclasses = getObjectDefSubclasses(dbms, TableDef.class);
		Object[] tableConstrutorParameters = new Object[constructorParameters.length+1];
		tableConstrutorParameters[0] = dbms;
		for (int  p=0; p<constructorParameters.length; p++)
			tableConstrutorParameters[p+1] = constructorParameters[p];
		return newTableDef((Class<T>) (subclasses.isEmpty() ? SQLTableDef.class : subclasses.iterator().next()), tableConstrutorParameters);
	}

	/**
	 * <p>Create and instance of a IndexDef subclass for an specific relational database management system.</p>
	 * @param dbms RDBMS
	 * @param constructorParameters Object&hellip; Parameters for the IndexDef subclass constructor
	 * @return &lt;? extends TableDef&gt; The returned type will be SQLIndex unless another subclass of ViewDef has been defined for the RDBMS
	 * @throws NoSuchMethodException If no suitable constructor can be found for the given parameters
	 * @throws JDOException
	 */
	@SuppressWarnings("unchecked")
	public static <T extends IndexDef> T newIndexDef(RDBMS dbms, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		Set<Class<?>> subclasses = getObjectDefSubclasses(dbms, TableDef.class);
		Object[] tableConstrutorParameters = new Object[constructorParameters.length+1];
		tableConstrutorParameters[0] = dbms;
		for (int  p=0; p<constructorParameters.length; p++)
			tableConstrutorParameters[p+1] = constructorParameters[p];
		return newIndexDef((Class<T>) (subclasses.isEmpty() ? SQLIndex.class : subclasses.iterator().next()), tableConstrutorParameters);
	}

	/**
	 * <p>Create and instance of a ProcedureDef subclass for an specific relational database management system.</p>
	 * @param dbms RDBMS
	 * @param constructorParameters Object&hellip; Parameters for the ProcedureDef subclass constructor
	 * @return &lt;? extends ProcedureDef&gt; The returned type will be ProcedureDef unless another subclass of ProcedureDef has been defined for the RDBMS
	 * @throws NoSuchMethodException If no suitable constructor can be found for the given parameters
	 * @throws JDOException
	 */
	@SuppressWarnings("unchecked")
	public static <T extends ProcedureDef> T newProcedureDef(RDBMS dbms, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		Set<Class<?>> subclasses = getObjectDefSubclasses(dbms, ProcedureDef.class);
		return newProcedureDef((Class<T>) (subclasses.isEmpty() ? ProcedureDef.class : subclasses.iterator().next()), constructorParameters);
	}

	/**
	 * <p>Create and instance of a TriggerDef subclass for an specific relational database management system.</p>
	 * @param dbms RDBMS
	 * @param constructorParameters Object&hellip; Parameters for the TriggerDef subclass constructor
	 * @return &lt;? extends TriggerDef&gt; The returned type will be TriggerDef unless another subclass of TriggerDef has been defined for the RDBMS
	 * @throws NoSuchMethodException If no suitable constructor can be found for the given parameters
	 * @throws JDOException
	 */
	@SuppressWarnings("unchecked")
	public static <T extends TriggerDef> T newTriggerDef(RDBMS dbms, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		Set<Class<?>> subclasses = getObjectDefSubclasses(dbms, TriggerDef.class);
		return newTriggerDef((Class<T>) (subclasses.isEmpty() ? TriggerDef.class : subclasses.iterator().next()), constructorParameters);
	}

	/**
	 * <p>Create and instance of a SequenceDef subclass for an specific relational database management system.</p>
	 * @param dbms RDBMS
	 * @param constructorParameters Object&hellip; Parameters for the SequenceDef subclass constructor
	 * @return &lt;? extends TriggerDef&gt; The returned type will be SequenceDef unless another subclass of SequenceDef has been defined for the RDBMS
	 * @throws NoSuchMethodException If no suitable constructor can be found for the given parameters
	 * @throws JDOException
	 */
	@SuppressWarnings("unchecked")
	public static <T extends SequenceDef> T newSequenceDef(RDBMS dbms, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		Set<Class<?>> subclasses = getObjectDefSubclasses(dbms, SequenceDef.class);
		return newSequenceDef((Class<T>) (subclasses.isEmpty() ? SequenceDef.class : subclasses.iterator().next()), constructorParameters);
	}

	private static final Set<Class<?>> EMPTY_SET = Collections.unmodifiableSet(new HashSet<Class<?>>());
	
	private static Set<Class<?>> getObjectDefSubclasses(RDBMS dbms, Class<?> objectClass) throws NoSuchMethodException, JDOException {
		final String packageName = "org.judal.jdbc."+dbms.shortName();
		try {
			Class.forName(packageName+".Loader");
		} catch (Exception ignore) { }
		Package pkg = Package.getPackage(packageName);
		if (pkg==null)
			return EMPTY_SET;
		else
			return getSubclassesOf(pkg, objectClass);
	}

}
