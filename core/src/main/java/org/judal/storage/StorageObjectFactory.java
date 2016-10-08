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

import com.knowgate.debug.DebugFile;

import com.knowgate.typeutils.ObjectFactory;

/**
 * <p>Factory for storage objects.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class StorageObjectFactory extends ObjectFactory {

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
		return retval;
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
		if (DebugFile.trace)
			DebugFile.writeln("StorageObjectFactory.newRecord("+recordClass.getName()+","+constructorParameters+")");
		Constructor<R> recordConstructor = (Constructor<R>) getConstructor(recordClass, getParameterClasses(constructorParameters));
		if (null==recordConstructor)
			throw new NoSuchMethodException("No suitable constructor found for "+recordClass.getName());
		return newRecord(recordConstructor, constructorParameters);
	}

}
