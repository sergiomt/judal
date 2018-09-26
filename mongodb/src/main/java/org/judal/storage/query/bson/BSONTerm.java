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

import java.math.BigDecimal;
import java.math.BigInteger;

import java.sql.Timestamp;

import java.util.*;

import org.judal.metadata.NameAlias;
import org.judal.storage.query.Expression;
import org.judal.storage.query.Part;
import org.judal.storage.query.Term;

import static org.judal.storage.query.Operator.*;

import com.mongodb.client.model.Filters;

import org.bson.conversions.Bson;

/*
* Represents a fragment of the filter clause of a BSON document
* @author Sergio Montoro Ten
* @version 1.0
*/
public class BSONTerm extends Term {

	private static final long serialVersionUID = 1L;

	public BSONTerm(String columnName, String operator) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator);
	}

	public BSONTerm(String columnName, String operator, Object columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, Expression columnExpr) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnExpr);
	}

	public BSONTerm(String columnName, String operator, String columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, Boolean columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}
	
	public BSONTerm(String columnName, String operator, Short columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, Integer columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, Long columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, Float columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, Double columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, BigDecimal columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, BigInteger columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, boolean columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, short columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, int columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, long columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, float columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public BSONTerm(String columnName, String operator, double columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}
	
	public BSONTerm(String columnName, String operator, String tableName, Part nestedTerm) {
		super (columnName, operator, tableName, nestedTerm);
	}

	public BSONTerm(String columnName, String operator, NameAlias aliasedTableName, Part nestedTerm) {
		super (columnName, operator, aliasedTableName, nestedTerm);
	}
	
	public BSONTerm(String columnName, String operator, String tableName, String nestedColumnName, Part nestedTerm) {
		super (columnName, operator, tableName, nestedColumnName, nestedTerm);
	}

	public BSONTerm(String columnName, String operator, NameAlias aliasedTableName, String nestedColumnName, Part nestedTerm) {
		super (columnName, operator, aliasedTableName, nestedColumnName, nestedTerm);
	}

	public BSONTerm(String columnName, String operator, Collection<Object> columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, LinkedList<Object> columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, ArrayList<Object> columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, String[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, Integer[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, int[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, Long[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, long[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, Float[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, float[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, Double[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, double[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, Date[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, Calendar[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, java.sql.Date[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, Timestamp[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public BSONTerm(String columnName, String operator, BigDecimal[] columnValues) {
		super (columnName, operator, columnValues);
	}
	
	public BSONTerm(String columnName, String operator, double d1, double d2, double d3) {
		super (columnName, operator, d1, d2, d3);
	}

	public BSONTerm(String columnName, String operator, String tableName, String joinColumn, BSONTerm joinTerm) {
		super(columnName, operator, tableName,  joinColumn, joinTerm);
	}

	public BSONTerm(String columnName, String operator, NameAlias aliasedTableName, String joinColumn, BSONTerm joinTerm) {
		super(columnName, operator, aliasedTableName, joinColumn, joinTerm);
	}

	protected BSONTerm() { }

	@Override
	public BSONTerm clone() {
		BSONTerm theClone = new BSONTerm();
		theClone.clone(this);
		return theClone;
	}

	@Override
	public String getTableName() {
		return oAliasedTable.getAlias()==null ? oAliasedTable.getName() : oAliasedTable.getName()+" "+oAliasedTable.getAlias();
	}

	/**
	 * Get the term as a Bson including all parameter values
	 * @return Bson
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Bson getText() {
		Bson f;
		switch (getOperator()) {
		case EQ:
			f = Filters.eq(getColumnName(), getValue());
			break;
		case NEQ :
			f = Filters.ne(getColumnName(), getValue());
			break;
		case GT:
			f = Filters.gt(getColumnName(), getValue());
			break;
		case GTE:
			f = Filters.gte(getColumnName(), getValue());
			break;
		case LT:
			f = Filters.lt(getColumnName(), getValue());
			break;
		case LTE:
			f = Filters.gte(getColumnName(), getValue());
			break;
		case EXISTS:
			if (getValue()==null)
				f = Filters.exists(getColumnName());
			else if (getValue() instanceof Boolean)
				f = Filters.exists(getColumnName(), (Boolean) getValue());
			else
				throw new IllegalArgumentException("Term value must be of boolean type for operator EXISTS");
			break;
		case NOTEXISTS:
			if (getValue()==null)
				f = Filters.exists(getColumnName(), Boolean.FALSE);
			else if (getValue() instanceof Boolean)
				f = Filters.exists(getColumnName(), !(Boolean) getValue());
			else
				throw new IllegalArgumentException("Term value must be of boolean type for operator EXISTS");
			break;
		case IN:
			f = Filters.in(getColumnName(), (Iterable) getValue());
			break;
		case NOTIN:
			f = Filters.nin(getColumnName(), (Iterable) getValue());
			break;
		default:
			throw new UnsupportedOperationException("Operator " + getOperator() +  " is not supported by Mongo queries");
		}
		return f;
	}

	/**
	 * Get the term as a String
	 * @return String
	 */
	@Override	
	public String getTextParametrized() {
		return getText().toString();
	}

}
