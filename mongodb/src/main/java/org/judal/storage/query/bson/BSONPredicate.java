package org.judal.storage.query.bson;

/**
 * © Copyright 2018 the original author.
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

import org.bson.conversions.Bson;
import org.judal.storage.query.Part;
import org.judal.storage.query.Predicate;
import static org.judal.storage.query.Connective.*;

import com.knowgate.debug.DebugFile;
import com.knowgate.typeutils.ObjectFactory;
import com.mongodb.client.model.Filters;

public class BSONPredicate extends Predicate {

	private static final long serialVersionUID = 1L;

	public BSONPredicate() {
	}

	/**
	 * Add BSONTerm to predicate
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator
	 * @param oColumnValue Object Parameter value
	 * @return this QueryPredicate
	 */
	@Override
	@SuppressWarnings("unchecked")
	public BSONPredicate add(Object... constructorParameters)
		throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException,
			IllegalArgumentException {

		if (DebugFile.trace) {
			StringBuilder paramValues = new StringBuilder();
			if (constructorParameters!=null)
				for (Object o : constructorParameters)
					paramValues.append(o==null ? "null" : o.toString()).append(",");
			if (paramValues.length()>0) paramValues.setLength(paramValues.length()-1);
			DebugFile.writeln("Begin BSONPredicate.add(" + paramValues.toString() + ")");
			DebugFile.incIdent();
		}
		
		Class<?>[] parameterClasses = assumeNullsAre(Object.class, constructorParameters);

		Constructor<BSONTerm> constructor = (Constructor<BSONTerm>) ObjectFactory.getConstructor(BSONTerm.class, parameterClasses);
		if (null==constructor) {
			constructor = (Constructor<BSONTerm>) ObjectFactory.getConstructor(BSONTerm.class, assumeNullsAre(Part.class, constructorParameters));
			if (null==constructor) {			
				StringBuilder paramClassNames = new StringBuilder();
				for (int p = 0; p<constructorParameters.length; p++)
					paramClassNames.append(parameterClasses[p].getName()).append(p<constructorParameters.length-1 ? "," : "");
				throw new NoSuchMethodException("Could not find suitable constructor for BSONTerm(" + paramClassNames.toString() + ")");
			}
		}

		BSONTerm term = null;
		try {
			term = constructor.newInstance(constructorParameters);
		} catch (InvocationTargetException e) {
			throw new InstantiationException(e.getMessage());
		}

		super.addPart(term);
		
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End BSONPredicate.add()");
		}
		
		return this;
	}

	private Class<?>[] assumeNullsAre(Class<?> classForNulls, Object[] constructorParameters) {
		Class<?>[] parameterClasses = new Class<?>[constructorParameters.length];
		for (int p = 0; p<constructorParameters.length; p++) {
			if (constructorParameters[p]==null)
				parameterClasses[p] = classForNulls;
			else
				parameterClasses[p] = constructorParameters[p].getClass();
			if (Part.class.isAssignableFrom(parameterClasses[p]))
				parameterClasses[p] = Part.class;
		}
		return parameterClasses;
	}

	@Override
	public BSONPredicate clone() {
		BSONPredicate theClone = new BSONPredicate();
		theClone.clone(this);
		return theClone;
	}

	private String logicalConnective() {
		if (connective()==null)
			return null;
		switch(connective()) {
		case AND:
			return "AND";
		case OR:
			return "OR";
		default:
			throw new UnsupportedOperationException("Unrecognized connective "+connective());
		}
	}

	/**
	 * Get predicate as BSON text fragment including all parameter values
	 * @return Bson
	 */
	@Override
	public Bson getText() {
		final int nParts = size();
		if (nParts>0) {
			Bson[] bsonParts = new Bson[nParts];
			for (int p=0; p<nParts; p++)
				bsonParts[p] = (Bson) parts().get(p).getText();
			if (AND.toString().equalsIgnoreCase(logicalConnective()))
				return Filters.and(Filters.and(bsonParts));
			else if (OR.toString().equalsIgnoreCase(logicalConnective()))
				return Filters.or(Filters.and(bsonParts));
			else
				throw new IllegalArgumentException("Unrecognized logical oconnectiive "+logicalConnective());
		} // fi
		return null;
	}

	/**
	 * Get predicate as BSON text fragment with parameter values as question marks "?"
	 * @return String
	 */
	@Override
	public String getTextParametrized() {
		return getText().toString();
	}
	
}
