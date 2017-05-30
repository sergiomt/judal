package org.judal.storage;

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

import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;

import com.knowgate.debug.DebugFile;

import com.knowgate.typeutils.ObjectFactory;

/**
 * <p>Factory for storage objects.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class StorageObjectFactory extends ObjectFactory {

	@SuppressWarnings("rawtypes")
	private static Class JavaRecord;
	@SuppressWarnings("rawtypes")
	private static Class JavaRecordSet;
	@SuppressWarnings("rawtypes")
	private static Class ScalaRecord;
	@SuppressWarnings("rawtypes")
	private static Class ScalaRecordSet;

	static {
		try {
			JavaRecord = Class.forName("org.judal.storage.java.JavaRecord");
		} catch (ClassNotFoundException ignore) { }
		try {
			JavaRecordSet = Class.forName("org.judal.storage.java.ArrayListRecordSet");
		} catch (ClassNotFoundException ignore) { }
		try {
			ScalaRecord = Class.forName("org.judal.storage.scala.ScalaRecord");
		} catch (ClassNotFoundException ignore) { }
	}
	
	/**
	 * <p>Create instance of a class that implements Stored interface.</p>
	 * @param recordConstructor Constructor&lt;R extends Stored&gt;
	 * @param constructorParameters Object[] Parameters for the given constructor
	 * @return R extends Stored
	 * @throws NoSuchMethodException If constructor parameter classes do not match the classes of constructorParameters
	 */
	public static <R extends Stored> R newStored(Constructor<R> storedConstructor, Object... constructorParameters) {
		R retval = null;
		final int parameterCount = storedConstructor.getParameterCount();
		try {
			if (parameterCount==0)
				retval = storedConstructor.newInstance();
			else
				retval = storedConstructor.newInstance(filterParameters(storedConstructor.getParameters(), constructorParameters));
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException | IllegalArgumentException xcpt) {
			if (DebugFile.trace)
				DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage()+" StorageObjectFactory.newStored(Constructor, "+(constructorParameters.length==0 ? "" : constructorParameters)+")");
		}
		return retval;
	}

	/**
	 * <p>Create instance of a class that implements Record interface.</p>
	 * @param recordConstructor Constructor&lt;R extends Record&gt;
	 * @param constructorParameters Object[] Parameters for the given constructor
	 * @return R extends Record
	 * @throws NoSuchMethodException If constructor parameter classes do not match the classes of constructorParameters
	 */
	public static <R extends Record> R newRecord(Constructor<R> recordConstructor, Object... constructorParameters) {
		R retval = null;
		final int parameterCount = recordConstructor.getParameterCount();
		try {
			if (parameterCount==0)
				retval = recordConstructor.newInstance();
			else
				retval = recordConstructor.newInstance(filterParameters(recordConstructor.getParameters(), constructorParameters));
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException | IllegalArgumentException xcpt) {
			if (DebugFile.trace)
				DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage()+" StorageObjectFactory.newRecord(Constructor, "+(constructorParameters.length==0 ? "" : constructorParameters)+")");
		}
		if (DebugFile.trace)
			DebugFile.writeln("StorageObjectFactory.newRecord() : " + retval.getClass().getName());
		return retval;
	}

	/**
	 * <p>Create instance of a class that implements Stored interface.</p>
	 * @param recordClass Class&lt;R extends Stored&gt;
	 * @param constructorParameters Object[] Parameters for the class constructor
	 * @return R extends Stored
	 * @throws NoSuchMethodException If no constructor was found allowing the given parameters
	 */
	@SuppressWarnings("unchecked")
	public static <R extends Stored> R newStored(Class<R> storedClass, Object... constructorParameters) throws NoSuchMethodException {
		if (DebugFile.trace)
			DebugFile.writeln("StorageObjectFactory.newRecord("+storedClass.getName()+","+constructorParameters+")");
		Constructor<R> storedConstructor = (Constructor<R>) getConstructor(storedClass, getParameterClasses(constructorParameters));
		if (null==storedConstructor)
			throw new NoSuchMethodException("No suitable constructor found for "+storedClass.getName());
		return newStored(storedConstructor, constructorParameters);
	}

	/**
	 * <p>Create instance of a class that implements Record interface.</p>
	 * @param recordClass Class&lt;R extends Record&gt;
	 * @param constructorParameters Object[] Parameters for the class constructor
	 * @return R extends Record
	 * @throws NoSuchMethodException If no constructor was found allowing the given parameters
	 */
	@SuppressWarnings("unchecked")
	public static <R extends Record> R newRecord(Class<R> recordClass, Object... constructorParameters) throws NoSuchMethodException {
		if (DebugFile.trace) {
			String constParams = "";
			if (constructorParameters!=null && constructorParameters.length!=0)
				for (int p=0; p<constructorParameters.length; p++)
					constParams += constructorParameters.getClass()==null ? "null," : constructorParameters[p].getClass().getName() + ",";
			DebugFile.writeln("StorageObjectFactory.newRecord("+recordClass.getName()+",["+constParams+"])");
		}
		Constructor<R> recordConstructor = (Constructor<R>) getConstructor(recordClass, getParameterClasses(constructorParameters));
		if (null==recordConstructor) {
			recordConstructor = (Constructor<R>) tryConstructor(recordClass);
			if (null==recordConstructor) {
				throw new NoSuchMethodException("StorageObjectFactory.newRecord() No suitable constructor found for "+recordClass.getName());
			} else {
				if (DebugFile.trace)
					DebugFile.writeln("no matching constructor found for "+recordClass.getName()+" using default constructor");				
			}
		}
		return newRecord(recordConstructor, constructorParameters);
	}
	
	public static <R extends Record> RecordSet<R> newRecordSet(Constructor<RecordSet<R>> recordsetConstructor, Object... constructorParameters) throws NoSuchMethodException {
		RecordSet<R> retval = null;
		final int parameterCount = recordsetConstructor.getParameterCount();
		try {
			if (parameterCount==0)
				retval = recordsetConstructor.newInstance();
			else
				retval = recordsetConstructor.newInstance(filterParameters(recordsetConstructor.getParameters(), constructorParameters));
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException | IllegalArgumentException xcpt) {
			if (DebugFile.trace)
				DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage()+" StorageObjectFactory.newRecordSet(Constructor, "+(constructorParameters.length==0 ? "" : constructorParameters)+")");
		}
		if (DebugFile.trace)
			DebugFile.writeln("StorageObjectFactory.newRecordSet() : " + retval.getClass().getName());
		return retval;
		
	}

	/**
	 * <p>Create instance of a class that implements RecordSet interface for the given Record class.</p>
	 * @param recordClass Class&lt;R extends Record&gt;
	 * @param constructorParameters Object[] Parameters for the class constructor
	 * @return RecordSet&lt;R extends Record&gt;
	 * @throws NoSuchMethodException If no constructor was found allowing the given parameters
	 */
	@SuppressWarnings("unchecked")
	public static <R extends Record> RecordSet<R> newRecordSetOf(Class<R> recordClass, Object... constructorParameters) throws NoSuchMethodException {
		Constructor<RecordSet<R>> recordsetConstructor;
		Class<RecordSet<R>> recordsetClass;
		if (ScalaRecord!=null && recordClass.isAssignableFrom(ScalaRecord))
			recordsetClass = ScalaRecordSet;
		else
			recordsetClass = JavaRecordSet;
		recordsetConstructor = (Constructor<RecordSet<R>>) getConstructor(recordsetClass, getParameterClasses(recordClass, constructorParameters));
		if (null==recordsetConstructor) {
			recordsetConstructor = (Constructor<RecordSet<R>>) tryConstructor(recordClass);
			if (null==recordsetConstructor) {
				throw new NoSuchMethodException("StorageObjectFactory.newRecordSetOf() No suitable constructor found for "+recordsetClass.getName());
			} else {
				if (DebugFile.trace)
					DebugFile.writeln("no matching constructor found for "+recordClass.getName()+" using default constructor");				
			}
		}
		Object[] constParameters;
		if (constructorParameters==null || constructorParameters.length==0) {
			constParameters = new Object[]{recordClass};
		} else {
			constParameters = new Object[constructorParameters.length+1];
			constParameters[0] = recordClass;
			for (int p=0; p<constructorParameters.length; p++)
				constParameters[p+1] = constructorParameters[p];
		}
			
		return newRecordSet(recordsetConstructor, constParameters);
	}
}
