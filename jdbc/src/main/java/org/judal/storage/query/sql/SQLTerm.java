package org.judal.storage.query.sql;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.sql.Timestamp;

import java.util.Calendar;
import java.util.Date;

import org.judal.metadata.NameAlias;
import org.judal.storage.query.Expression;
import org.judal.storage.query.Operator;
import org.judal.storage.query.Part;
import org.judal.storage.query.Predicate;
import org.judal.storage.query.Term;

import org.judal.jdbc.metadata.SQLFunctions;

/*
* Represents a fragment of the WHERE clause of a SQL statement
* @author Sergio Montoro Ten
* @version 1.0
*/
public class SQLTerm extends Term {

	private static final long serialVersionUID = 1L;

	private SQLFunctions sqlFuncts;

	public SQLTerm(String columnName, String operator) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator);
	}

	public SQLTerm(String columnName, String operator, Object columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, Expression columnExpr) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnExpr);
	}

	public SQLTerm(String columnName, String operator, String columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, Boolean columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}
	
	public SQLTerm(String columnName, String operator, Short columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, Integer columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, Long columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, Float columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, Double columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, BigDecimal columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, BigInteger columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, boolean columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, short columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, int columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, long columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, float columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}

	public SQLTerm(String columnName, String operator, double columnValue) throws ArrayIndexOutOfBoundsException {
		super (columnName, operator, columnValue);
	}
	
	public SQLTerm(String columnName, String operator, String tableName, Part nestedTerm) {
		super (columnName, operator, tableName, nestedTerm);
	}

	public SQLTerm(String columnName, String operator, NameAlias aliasedTableName, Part nestedTerm) {
		super (columnName, operator, aliasedTableName, nestedTerm);
	}
	
	public SQLTerm(String columnName, String operator, String tableName, String nestedColumnName, Part nestedTerm) {
		super (columnName, operator, tableName, nestedColumnName, nestedTerm);
	}

	public SQLTerm(String columnName, String operator, NameAlias aliasedTableName, String nestedColumnName, Part nestedTerm) {
		super (columnName, operator, aliasedTableName, nestedColumnName, nestedTerm);
	}
	
	public SQLTerm(String columnName, String operator, String[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Integer[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, int[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Long[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, long[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Float[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, float[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Double[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, double[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Date[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Calendar[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, java.sql.Date[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, Timestamp[] columnValues) {
		super (columnName, operator, columnValues);
	}

	public SQLTerm(String columnName, String operator, BigDecimal[] columnValues) {
		super (columnName, operator, columnValues);
	}
	
	public SQLTerm(String columnName, String operator, double d1, double d2, double d3) {
		super (columnName, operator, d1, d2, d3);
	}

	public SQLTerm(String columnName, String operator, String tableName, String joinColumn, SQLTerm joinTerm) {
		super(columnName, operator, tableName,  joinColumn, joinTerm);
	}

	public SQLTerm(String columnName, String operator, NameAlias aliasedTableName, String joinColumn, SQLTerm joinTerm) {
		super(columnName, operator, aliasedTableName, joinColumn, joinTerm);
	}

	protected SQLTerm() { }

	public SQLFunctions getSQLFunctions() {
		return sqlFuncts;
	}

	public void setSQLFunctions(final SQLFunctions sqlFuncts) {
		this.sqlFuncts = sqlFuncts;
	}

	@Override
	public SQLTerm clone() {
		SQLTerm theClone = new SQLTerm();
		theClone.clone(this);
		theClone.setSQLFunctions(getSQLFunctions());
		return theClone;
	}

	@Override
	public String getTableName() {
		return oAliasedTable.getAlias()==null ? oAliasedTable.getName() : oAliasedTable.getName()+" "+oAliasedTable.getAlias();
	}

	/**
	 * Get the term as a String including all parameter values
	 * @return String
	 */
	@Override
	public String getText() {
		StringBuilder oTxt = new StringBuilder(254);
		final Object value0 = (aValues==null || aValues.length<1 ? null : aValues[0]);
		final Object value1 = (aValues==null || aValues.length<2 ? null : aValues[1]);
		if (value0 instanceof Term || value0 instanceof Predicate) {
			if (sNestedColumn==null)
				throw new NullPointerException("Column name for subquery cannot be null");
			Part oNested = (Part) value0;
			if (sOper.equalsIgnoreCase(Operator.EXISTS) || sOper.equalsIgnoreCase(Operator.NOTEXISTS))
				if (null==oNested)
					oTxt.append(sOper).append(" (SELECT ").append(sNestedColumn).append(" FROM ").append(getTableName()).append(")");
				else
					oTxt.append(sOper).append(" (SELECT ").append(sNestedColumn).append(" FROM ").append(getTableName()).append(" WHERE ").append(oNested.getText()).append(")");
			
		} else {
			if (sOper.equalsIgnoreCase(Operator.WITHIN)) {
				final Object value2 = (aValues==null || aValues.length<3 ? null : aValues[2]);
				oTxt.append(Operator.WITHIN).append(" (").append(sColumn).append(", ST_GeographyFromText('SRID=4326;POINT(").append(toSQLString(value0)).append(" ").append(toSQLString(value1)+")'),").append(toSQLString(value2)).append(") ");
			} else if (sOper.equalsIgnoreCase(Operator.BETWEEN)) {
				oTxt.append(sColumn).append(" ").append(Operator.BETWEEN).append(" ").append(toSQLString(value0)).append(" AND ").append(toSQLString(value1)).append(" ");
			} else if (sOper.equalsIgnoreCase(Operator.EXISTS) || sOper.equalsIgnoreCase(Operator.NOTEXISTS)) {
				oTxt.append(sOper).append(" (").append(replaceParamaters(sColumn)).append(") ");
			} else if (sOper.equalsIgnoreCase(Operator.ISNULL) || sOper.equalsIgnoreCase(Operator.ISNOTNULL)) {
				oTxt.append(sColumn).append(" ").append(sOper);
			} else if (sOper.equalsIgnoreCase(Operator.IS) || sOper.equalsIgnoreCase(Operator.ISNOT)) {
				oTxt.append(sColumn).append(" ").append(sOper).append(" ").append(toSQLString(value0));
			} else if (sOper.equalsIgnoreCase(Operator.IN) || sOper.equalsIgnoreCase(Operator.NOTIN)) {
				if (null==aValues || aValues.length==0) {
					oTxt.append(sColumn).append(" ").append(sOper).append("()");
				} else {
					oTxt.append(sColumn).append(" ").append(sOper).append(" (").append(toSQLString(aValues[0]));
					for (int v = 1; v < nValues; v++)
						oTxt.append(",").append(toSQLString(aValues[v]));
					oTxt.append(")");
				}
			} else {
				oTxt.append(sColumn).append(" ").append(sOper).append(" ").append(toSQLString(value0));
			}
		}
		return oTxt.toString();
	}

	private String toSQLString(Object value) {
		if (null==value)
			return null;
		else if (value instanceof String)
			return "'" + value + "'";
		else if (value instanceof Timestamp)
			return getSQLFunctions().escape((Timestamp) value);
		else if (value instanceof java.sql.Date)
			return getSQLFunctions().escape((java.sql.Date) value);
		else if (value instanceof java.util.Date)
			return getSQLFunctions().escape((java.util.Date) value, "ts");
		else if (value instanceof java.util.Calendar)
			return getSQLFunctions().escape((java.util.Calendar) value, "ts");
		else
			return value.toString();
	}

	/**
	 * Get the term as a String with parameters as SQL question marks
	 * @return String
	 */
	@Override	
	public String getTextParametrized() {
		StringBuilder oTxt = new StringBuilder(254);
		final Object value0 = (aValues==null ? null : aValues[0]);
		if (value0 instanceof Term || value0 instanceof Predicate) {
			if (sNestedColumn==null)
				throw new NullPointerException("Column name for subquery cannot be null");
			Part oNested = (Part) value0;
			if (sOper.equalsIgnoreCase(Operator.EXISTS) || sOper.equalsIgnoreCase(Operator.NOTEXISTS))
				if (null==oNested)
					oTxt.append(sOper).append(" (SELECT ").append(sNestedColumn).append(" FROM ").append(getTableName()).append(")");
				else
					oTxt.append(sOper).append(" (SELECT ").append(sNestedColumn).append(" FROM ").append(getTableName()).append(" WHERE ").append(oNested.getTextParametrized()).append(")");
			else
				oTxt.append(sColumn).append(" ").append(sOper).append(" (SELECT ").append(sNestedColumn).append(" FROM ").append(getTableName()).append(" WHERE ").append(oNested.getTextParametrized()).append(")");
		} else if (value0 instanceof Expression) {
			oTxt.append(sColumn).append(" ").append(sOper).append(" ").append(value0.toString());
		} else {
			if (sOper.equalsIgnoreCase(Operator.WITHIN)) {
				oTxt.append(Operator.WITHIN).append("(").append(sColumn).append(", ST_SetSRID(ST_MakePoint("+q(aValues[0])+","+q(aValues[1])+"),4326),"+q(aValues[2])+")");
			} else if (sOper.equalsIgnoreCase(Operator.BETWEEN)) {
				oTxt.append(sColumn).append(" ").append(Operator.BETWEEN).append(" ").append(q(aValues[0])).append(" AND ").append(q(aValues[1])+" ");
			} else if (sOper.equalsIgnoreCase(Operator.EXISTS) || sOper.equalsIgnoreCase(Operator.NOTEXISTS)) {
				oTxt.append(sOper).append(" (").append(sColumn).append(")");
			} else if (sOper.equalsIgnoreCase(Operator.ISNULL) || sOper.equalsIgnoreCase(Operator.ISNOTNULL)) {
				oTxt.append(sColumn).append(" ").append(sOper);
			} else if (sOper.equalsIgnoreCase(Operator.IS) || sOper.equalsIgnoreCase(Operator.ISNOT)) {
				oTxt.append(sColumn).append(" ").append(sOper).append(" ").append((value0==null ? "NULL" : q(value0)));
			} else if (sOper.equalsIgnoreCase(Operator.IN) || sOper.equalsIgnoreCase(Operator.NOTIN)) {
				oTxt.append(sColumn).append(" ").append(sOper).append(" (").append(q(value0));
				for (int v=1; v<nValues; v++)
					oTxt.append(",").append(q(aValues[v]));
				oTxt.append(")");
			} else {
				oTxt.append(sColumn).append(" ").append(sOper).append(" ").append(q(value0));
			}
		}
		return oTxt.toString();
	}

	private String q(Object exprOrVal) {
		if (exprOrVal instanceof Expression)
			return exprOrVal.toString();
		else
			return "?";
	}

}
