package org.judal.metadata;

/**
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jdo.JDOUserException;

import org.judal.storage.table.Record;

/**
 * Helper class from parsing "<i>name</i>.<i>alias</i>" or "<i>name</i> AS <i>alias</i>" strings
 * @author Sergio Montoro Ten
 *
 */
public class NameAlias {

	private static final int columnNameGroup = 3;
	private static final int columnAliasGroup = 4;
	private static final String tableAlias = "((\"[\\w ]+\"|[\\w ]+)\\x2E)*";
	private static final Pattern quotedAliased = Pattern.compile(tableAlias+"(\".+\") +(.)", Pattern.CASE_INSENSITIVE);
	private static final Pattern unquotedAliased = Pattern.compile(tableAlias+"(.+) +(.+)", Pattern.CASE_INSENSITIVE);
	private static final Pattern quotedAsAlias = Pattern.compile(tableAlias+"\"(.+)\" + AS +(.+)", Pattern.CASE_INSENSITIVE);
	private static final Pattern unquotedAsAlias = Pattern.compile(tableAlias+"(.+) + AS +(.+)", Pattern.CASE_INSENSITIVE);
	private static final Pattern quotedUnAliased = Pattern.compile(tableAlias+"\".+\"", Pattern.CASE_INSENSITIVE);

	private static Pattern[] namePlusAliasPatterns = new Pattern[]{quotedAsAlias, quotedAliased, unquotedAsAlias, unquotedAliased};

	private String name;
	private String alias;
	
	public NameAlias(String name, String alias) {
		this.name = name;
		this.alias = alias;
	}

	/**
	 * <p>Get name part.</p>
	 * @return Substring to the left of "." or "AS"
	 */
	public String getName() {
		return name;
	}

	/**
	 * <p>Get alias part (if present).</p>
	 * Substring to the left of "." or "AS"
	 * @return Substring to the right of "." or "AS". Or <b>null</b> if the parsed string has no alias.
	 */
	public String getAlias() {
		return alias;
	}

	/**
	 * @return String "<i>name</i> AS <i>alias</i>"
	 */
	public String toString() {
		if (null==alias || alias.isEmpty())
			return name;
		else
			return name + " AS " +alias;
	}

	/**
	 * Split a column name and alias from a string.
	 * The input string must be of the form:
	 * "columnName"
	 * "tableName"."columnName"
	 * "tableName".columnName" "columnAlias"
	 * "tableName".columnName" AS "columnAlias"
	 * Double quotes are optional for tableName, columnName and columnAlias if they do not contain spaces
	 * @param aliasedName String 
	 * @return NameAlias
	 * @throws JDOUserException if given aliasedName does not match any of the recognized patterns
	 */
	public static NameAlias parse(String aliasedName) throws JDOUserException {
		final int space = aliasedName.indexOf(' ');
		final int quote = aliasedName.indexOf('"');
		if (space<0 && quote<0) {
			return new NameAlias(aliasedName, null);
		} else {
			for (Pattern pattern : namePlusAliasPatterns) {
				Matcher mtchr = pattern.matcher(aliasedName);
				if (mtchr.matches())
					return new NameAlias(mtchr.group(columnNameGroup), mtchr.group(columnAliasGroup));
			}
			if (quotedUnAliased.matcher(aliasedName).matches())
				return new NameAlias(aliasedName, null);
		}
		throw new JDOUserException("Malformed name "+aliasedName);
	}
	
	/**
	 * Shortcut for new NameAlias(name, alias)
	 * @param name String
	 * @param alias String
	 * @return NameAlias
	 */
	public static NameAlias AS(String name, String alias) {
		return new NameAlias(name, alias);
	}

	/**
	 * Shortcut for new NameAlias(record.getTableName(), alias)
	 * @param record Record
	 * @param alias String
	 * @return NameAlias
	 */
	public static NameAlias AS(Record rec, String alias) {
		return new NameAlias(rec.getTableName(), alias);
	}

}
