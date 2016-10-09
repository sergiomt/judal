package org.judal.metadata;

/**
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

/**
 * <b>Extensible metadata object factory.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
import com.knowgate.typeutils.ObjectFactory;;

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
			DebugFile.writeln("StorageObjectFactory.newTableDef("+tableDefSubclass.getName()+","+constructorParameters+")");
		@SuppressWarnings("unchecked")
		Constructor<T> tableDefConstructor = (Constructor<T>) getConstructor(tableDefSubclass, getParameterClasses(constructorParameters));
		if (null==tableDefConstructor)
			throw new NoSuchMethodException("No suitable constructor found for "+tableDefSubclass.getName());
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
	 * @param tableDefSubclass Subclass of org.judal.metadata.ViewDef
	 * @param constructorParameters Parameters for the constructor
	 * @return T extends ViewDef
	 * @throws NoSuchMethodException
	 * @throws JDOException
	 */
	public static <T extends ViewDef> T newViewDef(Class<T> viewDefSubclass, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		if (DebugFile.trace)
			DebugFile.writeln("StorageObjectFactory.newTableDef("+viewDefSubclass.getName()+","+constructorParameters+")");
		@SuppressWarnings("unchecked")
		Constructor<T> viewDefConstructor = (Constructor<T>) getConstructor(viewDefSubclass, getParameterClasses(constructorParameters));
		if (null==viewDefConstructor)
			throw new NoSuchMethodException("No suitable constructor found for "+viewDefSubclass.getName());
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
	 * <p>Find a suitable constructor using reflection and create an instance of a subclass of ProcedureDef</p>
	 * @param tableDefSubclass Subclass of org.judal.metadata.ProcedureDef
	 * @param constructorParameters Parameters for the constructor
	 * @return T extends ProcedureDef
	 * @throws NoSuchMethodException
	 * @throws JDOException
	 */
	public static <T extends ProcedureDef> T newProcedureDef(Class<T> procedureDefSubclass, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		if (DebugFile.trace)
			DebugFile.writeln("StorageObjectFactory.newProcedureDef("+procedureDefSubclass.getName()+","+constructorParameters+")");
		@SuppressWarnings("unchecked")
		Constructor<T> procedureDefConstructor = (Constructor<T>) getConstructor(procedureDefSubclass, getParameterClasses(constructorParameters));
		if (null==procedureDefConstructor)
			throw new NoSuchMethodException("No suitable constructor found for "+procedureDefSubclass.getName());
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
	 * @param tableDefSubclass Subclass of org.judal.metadata.TriggerDef
	 * @param constructorParameters Parameters for the constructor
	 * @return T extends TriggerDef
	 * @throws NoSuchMethodException
	 * @throws JDOException
	 */
	public static <T extends TriggerDef> T newTriggerDef(Class<T> triggerDefSubclass, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		if (DebugFile.trace)
			DebugFile.writeln("StorageObjectFactory.newTriggerDef("+triggerDefSubclass.getName()+","+constructorParameters+")");
		@SuppressWarnings("unchecked")
		Constructor<T> triggerDefConstructor = (Constructor<T>) getConstructor(triggerDefSubclass, getParameterClasses(constructorParameters));
		if (null==triggerDefConstructor)
			throw new NoSuchMethodException("No suitable constructor found for "+triggerDefSubclass.getName());
		T retval = null;
		final int parameterCount = triggerDefConstructor.getParameterCount();
		try {
			if (parameterCount==0)
				retval = triggerDefConstructor.newInstance();
			else
				retval = triggerDefConstructor.newInstance(filterParameters(triggerDefConstructor.getParameters(), constructorParameters));
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException | IllegalArgumentException xcpt) {
			if (DebugFile.trace)
				DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage()+" MetadataObjectFactory.newTriggerDef(Constructor, "+(constructorParameters.length==0 ? "" : constructorParameters)+")");
		}
		return retval;
	}

	/**
	 * <p>Find a suitable constructor using reflection and create an instance of a subclass of SequenceDef</p>
	 * @param tableDefSubclass Subclass of org.judal.metadata.SequenceDef
	 * @param constructorParameters Parameters for the constructor
	 * @return T extends SequenceDef
	 * @throws NoSuchMethodException
	 * @throws JDOException
	 */
	public static <T extends SequenceDef> T newSequenceDef(Class<T> sequenceDefSubclass, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		if (DebugFile.trace)
			DebugFile.writeln("StorageObjectFactory.newSequenceDef("+sequenceDefSubclass.getName()+","+constructorParameters+")");
		@SuppressWarnings("unchecked")
		Constructor<T> sequenceDefConstructor = (Constructor<T>) getConstructor(sequenceDefSubclass, getParameterClasses(constructorParameters));
		if (null==sequenceDefConstructor)
			throw new NoSuchMethodException("No suitable constructor found for "+sequenceDefSubclass.getName());
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