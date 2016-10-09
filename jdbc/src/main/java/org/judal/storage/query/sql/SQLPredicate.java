package org.judal.storage.query.sql;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.judal.storage.query.Part;
import org.judal.storage.query.Predicate;

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
	public SQLPredicate add(Object... constructorParameters)
		throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Class<?>[] parameterClasses = new Class<?>[constructorParameters.length];
		for (int p = 0; p<constructorParameters.length; p++) {
			if (constructorParameters[p]==null)
				parameterClasses[p] = Object.class;
			else
				parameterClasses[p] = constructorParameters[p].getClass();
			if (Part.class.isAssignableFrom(parameterClasses[p]))
				parameterClasses[p] = Part.class;
		}
		Constructor<SQLTerm> constructor = SQLTerm.class.getConstructor(parameterClasses);
		super.add(constructor.newInstance(constructorParameters));
		return this;
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
		StringBuffer oBuff = new StringBuffer();
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
		StringBuffer oBuff = new StringBuffer();
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
