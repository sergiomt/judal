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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import javax.jdo.JDOException;

import com.knowgate.debug.DebugFile;
import com.knowgate.typeutils.ObjectFactory;

/**
* Factory of metadata objects.
* @author Sergio Montoro Ten
* @version 1.0
*/
public class MetadataObjectFactory extends ObjectFactory {
	
	/**
	 * <p>Find a suitable constructor using reflection and create an instance of a subclass of TableDef</p>
	 * @param tableDefSubclass Subclass of org.judal.metadata.TableDef
	 * @param constructorParameters Parameters for the constructor
	 * @return T extends TableDef
	 * @throws NoSuchMethodException
	 * @throws JDOException
	 */
	public static <T extends TableDef> T newTableDef(Class<T> tableDefSubclass, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		if (DebugFile.trace)
			DebugFile.writeln("MetadataObjectFactory.newTableDef("+tableDefSubclass.getName()+"," + constructorParameters + ")");
		@SuppressWarnings("unchecked")
		Constructor<T> tableDefConstructor = (Constructor<T>) getConstructor(tableDefSubclass, getParameterClasses(constructorParameters));
		if (null==tableDefConstructor)
			throw new NoSuchMethodException("MetadataObjectFactory.newTableDef No suitable constructor found for "+tableDefSubclass.getName());
		T retval = null;
		final int parameterCount = tableDefConstructor.getParameterCount();
		try {
			if (parameterCount==0)
				retval = tableDefConstructor.newInstance();
			else
				retval = tableDefConstructor.newInstance(filterParameters(tableDefConstructor.getParameters(), constructorParameters));
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException | IllegalArgumentException xcpt) {
			if (DebugFile.trace)
				DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage()+" MetadataObjectFactory.newTableDef(Constructor, "+(constructorParameters.length==0 ? "" : constructorParameters)+")");
		}
		return retval;
	}

	/**
	 * <p>Find a suitable constructor using reflection and create an instance of a subclass of ViewDef</p>
	 * @param viewDefSubclass Subclass of org.judal.metadata.ViewDef
	 * @param constructorParameters Parameters for the constructor
	 * @return T extends ViewDef
	 * @throws NoSuchMethodException
	 * @throws JDOException
	 */
	public static <T extends ViewDef> T newViewDef(Class<T> viewDefSubclass, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		if (DebugFile.trace) {
			StringBuilder paramClasses = new StringBuilder();
			if (null!=constructorParameters && constructorParameters.length>0) {
				paramClasses.append(constructorParameters[0].getClass().getName());
				for (int p=1; p<constructorParameters.length; p ++)
					paramClasses.append(",").append(constructorParameters[p].getClass().getName());					
			}
			DebugFile.writeln("MetadataObjectFactory.newViewDef("+viewDefSubclass.getName()+","+paramClasses.toString()+")");
		}
		@SuppressWarnings("unchecked")
		Constructor<T> viewDefConstructor = (Constructor<T>) getConstructor(viewDefSubclass, getParameterClasses(constructorParameters));
		if (null==viewDefConstructor)
			throw new NoSuchMethodException("MetadataObjectFactory.newViewDef No suitable constructor found for "+viewDefSubclass.getName());
		T retval = null;
		final int parameterCount = viewDefConstructor.getParameterCount();
		try {
			if (parameterCount==0)
				retval = viewDefConstructor.newInstance();
			else
				retval = viewDefConstructor.newInstance(filterParameters(viewDefConstructor.getParameters(), constructorParameters));
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException | IllegalArgumentException xcpt) {
			if (DebugFile.trace)
				DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage()+" MetadataObjectFactory.newViewDef(Constructor, "+(constructorParameters.length==0 ? "" : constructorParameters)+")");
		}
		return retval;
	}

	/**
	 * <p>Find a suitable constructor using reflection and create an instance of a subclass of IndexDef</p>
	 * @param indexDefSubclass Subclass of org.judal.metadata.IndexDef
	 * @param constructorParameters Parameters for the constructor
	 * @return T extends IndexDef
	 * @throws NoSuchMethodException
	 * @throws JDOException
	 */
	public static <T extends IndexDef> T newIndexDef(Class<T> indexDefSubclass, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		if (DebugFile.trace)
			DebugFile.writeln("MetadataObjectFactory.newIndexDef("+indexDefSubclass.getName()+","+constructorParameters+")");
		@SuppressWarnings("unchecked")
		Constructor<T> indexDefConstructor = (Constructor<T>) getConstructor(indexDefSubclass, getParameterClasses(constructorParameters));
		if (null==indexDefConstructor)
			throw new NoSuchMethodException("MetadataObjectFactory.newIndexDef No suitable constructor found for "+indexDefSubclass.getName());
		T retval = null;
		final int parameterCount = indexDefConstructor.getParameterCount();
		try {
			if (parameterCount==0)
				retval = indexDefConstructor.newInstance();
			else
				retval = indexDefConstructor.newInstance(filterParameters(indexDefConstructor.getParameters(), constructorParameters));
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException | IllegalArgumentException xcpt) {
			if (DebugFile.trace)
				DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage()+" MetadataObjectFactory.newIndexDef(Constructor, "+(constructorParameters.length==0 ? "" : constructorParameters)+")");
		}
		return retval;
	}
	
	/**
	 * <p>Find a suitable constructor using reflection and create an instance of a subclass of ProcedureDef</p>
	 * @param procedureDefSubclass Subclass of org.judal.metadata.ProcedureDef
	 * @param constructorParameters Parameters for the constructor
	 * @return T extends ProcedureDef
	 * @throws NoSuchMethodException
	 * @throws JDOException
	 */
	public static <T extends ProcedureDef> T newProcedureDef(Class<T> procedureDefSubclass, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		if (DebugFile.trace)
			DebugFile.writeln("MetadataObjectFactory.newProcedureDef("+procedureDefSubclass.getName()+","+constructorParameters+")");
		@SuppressWarnings("unchecked")
		Constructor<T> procedureDefConstructor = (Constructor<T>) getConstructor(procedureDefSubclass, getParameterClasses(constructorParameters));
		if (null==procedureDefConstructor)
			throw new NoSuchMethodException("MetadataObjectFactory.newProcedureDef No suitable constructor found for "+procedureDefSubclass.getName());
		T retval = null;
		final int parameterCount = procedureDefConstructor.getParameterCount();
		try {
			if (parameterCount==0)
				retval = procedureDefConstructor.newInstance();
			else
				retval = procedureDefConstructor.newInstance(filterParameters(procedureDefConstructor.getParameters(), constructorParameters));
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException | IllegalArgumentException xcpt) {
			if (DebugFile.trace)
				DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage()+" MetadataObjectFactory.newProcedureDef(Constructor, "+(constructorParameters.length==0 ? "" : constructorParameters)+")");
		}
		return retval;
	}

	/**
	 * <p>Find a suitable constructor using reflection and create an instance of a subclass of TriggerDef</p>
	 * @param triggerDefSubclass Subclass of org.judal.metadata.TriggerDef
	 * @param constructorParameters Parameters for the constructor
	 * @return T extends TriggerDef
	 * @throws NoSuchMethodException
	 * @throws JDOException
	 */
	public static <T extends TriggerDef> T newTriggerDef(Class<T> triggerDefSubclass, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		if (DebugFile.trace)
			DebugFile.writeln("MetadataObjectFactory.newTriggerDef("+triggerDefSubclass.getName()+","+constructorParameters+")");
		@SuppressWarnings("unchecked")
		Constructor<T> triggerDefConstructor = (Constructor<T>) getConstructor(triggerDefSubclass, getParameterClasses(constructorParameters));
		if (null==triggerDefConstructor)
			throw new NoSuchMethodException("MetadataObjectFactory.newTriggerDef No suitable constructor found for "+triggerDefSubclass.getName());
		T retval = null;
		final int parameterCount = triggerDefConstructor.getParameterCount();
		try {
			if (parameterCount==0)
				retval = triggerDefConstructor.newInstance();
			else
				retval = triggerDefConstructor.newInstance(filterParameters(triggerDefConstructor.getParameters(), constructorParameters));
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException | IllegalArgumentException xcpt) {
			if (DebugFile.trace)
				DebugFile.writeln(xcpt.getClass().getName() + " " + xcpt.getMessage() + " MetadataObjectFactory.newTriggerDef(Constructor, " + (constructorParameters.length==0 ? "" : constructorParameters)+")");
		}
		return retval;
	}

	/**
	 * <p>Find a suitable constructor using reflection and create an instance of a subclass of SequenceDef</p>
	 * @param sequenceDefSubclass Subclass of org.judal.metadata.SequenceDef
	 * @param constructorParameters Parameters for the constructor
	 * @return T extends SequenceDef
	 * @throws NoSuchMethodException
	 * @throws JDOException
	 */
	public static <T extends SequenceDef> T newSequenceDef(Class<T> sequenceDefSubclass, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		if (DebugFile.trace)
			DebugFile.writeln("MetadataObjectFactory.newSequenceDef("+sequenceDefSubclass.getName()+","+constructorParameters+")");
		@SuppressWarnings("unchecked")
		Constructor<T> sequenceDefConstructor = (Constructor<T>) getConstructor(sequenceDefSubclass, getParameterClasses(constructorParameters));
		if (null==sequenceDefConstructor)
			throw new NoSuchMethodException("MetadataObjectFactory.newSequenceDef No suitable constructor found for "+sequenceDefSubclass.getName());
		T retval = null;
		final int parameterCount = sequenceDefConstructor.getParameterCount();
		try {
			if (parameterCount==0)
				retval = sequenceDefConstructor.newInstance();
			else
				retval = sequenceDefConstructor.newInstance(filterParameters(sequenceDefConstructor.getParameters(), constructorParameters));
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException | IllegalArgumentException xcpt) {
			if (DebugFile.trace)
				DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage()+" MetadataObjectFactory.newSequenceDef(Constructor, "+(constructorParameters.length==0 ? "" : constructorParameters)+")");
		}
		return retval;
	}

}
