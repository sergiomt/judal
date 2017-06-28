package org.judal.storage.query;

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

public class Operator {

	public static final String NOTIN = "NOT IN";
	public static final String IN = "IN";
	public static final String IS = "IS";
	public static final String ISNOT = "IS NOT";
	public static final String ISNULL = "IS NULL";
	public static final String ISNOTNULL = "IS NOT NULL";
	public static final String EQ = "=";
	public static final String NEQ = "<>";
	public static final String LT = "<";
	public static final String GT = ">";
	public static final String LTE = "<=";
	public static final String GTE = ">=";
	public static final String EXISTS = "EXISTS";
	public static final String NOTEXISTS = "NOT EXISTS";
	public static final String BETWEEN = "BETWEEN";
	public static final String NOTBETWEEN = "NOT BETWEEN";
	public static final String LIKE = "LIKE";
	public static final String NOTLIKE = "NOT LIKE";
	public static final String ILIKE = "ILIKE";
	public static final String WITHIN = "ST_DWithin";
	public static final String NOTWITHIN = "NOT ST_DWithin";
	
}
