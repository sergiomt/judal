package org.judal.storage.query.sql;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.judal.storage.query.Part;
import org.judal.storage.query.Predicate;

import com.knowgate.debug.DebugFile;
import com.knowgate.typeutils.ObjectFactory;

public class SQLPredicate extends Predicate {

	private static final long serialVersionUID = 1L;

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
		throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		if (DebugFile.trace) {
			StringBuilder paramValues = new StringBuilder();
			if (constructorParameters!=null)
				for (Object o : constructorParameters)
					paramValues.append(o==null ? "null" : o.toString()).append(",");
			paramValues.setLength(paramValues.length()-1);
			DebugFile.writeln("Begin SQLPredicate.add(" + paramValues.toString() + ")");
			DebugFile.incIdent();
		}
		
		Class<?>[] parameterClasses = new Class<?>[constructorParameters.length];
		for (int p = 0; p<constructorParameters.length; p++) {
			if (constructorParameters[p]==null)
				parameterClasses[p] = Object.class;
			else
				parameterClasses[p] = constructorParameters[p].getClass();
			if (Part.class.isAssignableFrom(parameterClasses[p]))
				parameterClasses[p] = Part.class;
		}

		Constructor<SQLTerm> constructor = (Constructor<SQLTerm>) ObjectFactory.getConstructor(SQLTerm.class, parameterClasses);
		if (null==constructor) {
			StringBuilder paramClassNames = new StringBuilder();
				for (int p = 0; p<constructorParameters.length; p++)
					paramClassNames.append(parameterClasses[p].getName()).append(p<constructorParameters.length-1 ? "," : "");
			throw new NoSuchMethodException("Could not find suitable constructor for SQLTerm(" + paramClassNames.toString() + ")");
		}
		
		super.addPart(constructor.newInstance(constructorParameters));
		
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End SQLPredicate.add()");
		}
		
		return this;
	}

	@Override
	public SQLPredicate clone() {
		SQLPredicate theClone = new SQLPredicate();
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
