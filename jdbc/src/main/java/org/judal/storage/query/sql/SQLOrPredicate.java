package org.judal.storage.query.sql;

import org.judal.storage.query.Connective;

public class SQLOrPredicate extends SQLPredicate {

	private static final long serialVersionUID = 1L;

	@Override
	public Connective connective() {
		return Connective.OR;
	}

}
