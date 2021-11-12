package org.judal.storage.query.sql;

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

import org.judal.jdbc.metadata.SQLFunctions;

import org.judal.storage.query.Part;
import org.judal.storage.query.Predicate;

import com.knowgate.debug.DebugFile;
import com.knowgate.typeutils.ObjectFactory;

public class SQLPredicate extends Predicate {

	private static final long serialVersionUID = 1L;

	private SQLFunctions sqlFuncts;

	public SQLPredicate() {
		this.sqlFuncts = null;
	}

	public SQLPredicate(final SQLFunctions sqlFunctions) {
		setSQLFunctions(sqlFunctions);
	}

	public SQLFunctions getSQLFunctions() {
		return sqlFuncts;
	}

	public void setSQLFunctions(final SQLFunctions sqlFuncts) {
		this.sqlFuncts = sqlFuncts;
	}

	/**
	 * Add SQLTerm to predicate
	 * @param sColumnName String Column Name
	 * @param sOperator String Operator
	 * @param oColumnValue Object Parameter value
	 * @return this QueryPredicate
	 */
	@Override
	@SuppressWarnings("unchecked")
	public SQLPredicate add(Object... constructorParameters)
		throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException  {

		if (DebugFile.trace) {
			StringBuilder paramValues = new StringBuilder();
			if (constructorParameters!=null)
				for (Object o : constructorParameters)
					paramValues.append(o==null ? "null" : o.toString()).append(",");
			if (paramValues.length()>0) paramValues.setLength(paramValues.length()-1);
			DebugFile.writeln("Begin SQLPredicate.add(" + paramValues.toString() + ")");
			DebugFile.incIdent();
		}

		if (constructorParameters[0] instanceof SQLTerm) {
			if (constructorParameters.length==1) {
				super.addPart((SQLTerm) constructorParameters[0]);
			} else {
				for (int p=1; p<constructorParameters.length; p++)
					if (!(constructorParameters[0] instanceof SQLTerm))
						throw new IllegalArgumentException("SQLPredicate.add() if first parameter type is SQLTerm then every parameter must be SQLTerm as well");
				for (int p=0; p<constructorParameters.length; p++)
					super.addPart((SQLTerm) constructorParameters[p]);
			}
			
		}

		Class<?>[] parameterClasses = assumeNullsAre(Object.class, constructorParameters);

		Constructor<SQLTerm> constructor = (Constructor<SQLTerm>) ObjectFactory.getConstructor(SQLTerm.class, parameterClasses);
		if (null==constructor) {
			constructor = (Constructor<SQLTerm>) ObjectFactory.getConstructor(SQLTerm.class, assumeNullsAre(Part.class, constructorParameters));
			if (null==constructor) {			
				StringBuilder paramClassNames = new StringBuilder();
				for (int p = 0; p<constructorParameters.length; p++)
					paramClassNames.append(parameterClasses[p].getName()).append(p<constructorParameters.length-1 ? "," : "");
				throw new NoSuchMethodException("Could not find suitable constructor for SQLTerm(" + paramClassNames.toString() + ")");
			}
		}

		SQLTerm term = null;
		try {
			term = constructor.newInstance(constructorParameters);
		} catch (InvocationTargetException e) {
			throw new InstantiationException(e.getMessage());
		}
		term.setSQLFunctions(getSQLFunctions());

		super.addPart(term);
		
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End SQLPredicate.add()");
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
	public SQLPredicate clone() {
		SQLPredicate theClone = new SQLPredicate();
		theClone.clone(this);
		theClone.setSQLFunctions(getSQLFunctions());
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
	 * Get predicate as SQL text fragment including all parameter values
	 * @return String
	 */
	@Override
	public String getText() {
		StringBuilder oBuff = new StringBuilder();
		final int nParts = size();
		if (nParts>0) {
			oBuff.append(" ( ");
			oBuff.append(oParts.get(0).getText());
			for (int t=1; t<nParts; t++) {
				oBuff.append(" ").append(logicalConnective()).append(" ");
				oBuff.append(oParts.get(t).getText());
			} // next
			oBuff.append(" ) ");
		} // fi
		return oBuff.toString();
	}
	
	/**
	 * Get predicate as SQL text fragment with parameter values as question marks "?"
	 * @return String
	 */
	@Override
	public String getTextParametrized() {
		StringBuilder oBuff = new StringBuilder();
		final int nParts = size();
		if (nParts>0) {
			oBuff.append(" ( ");
			oBuff.append(oParts.get(0).getTextParametrized());
			for (int t=1; t<nParts; t++) {
				oBuff.append(" ").append(logicalConnective()).append(" ");
				oBuff.append(oParts.get(t).getTextParametrized());
			} // next
			oBuff.append(" ) ");
		} // fi
		return oBuff.toString();
	}
	
}
