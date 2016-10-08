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

import javax.jdo.metadata.QueryMetadata;

/**
 * JDO QueryMetadata interface implementation
 * @author Sergio Montoro Ten
 *
 */
public class QueryDef extends ExtendableDef implements QueryMetadata {

	private String name;
	private String query;
	private String language;
	private String planAccesor;
	private String resultClass;
	private boolean unique;
	private boolean unmodifiable;
	
	@Override
	public String getFetchPlan() {
		return planAccesor;
	}

	@Override
	public String getLanguage() {
		return language;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getQuery() {
		return query;
	}

	@Override
	public String getResultClass() {
		return resultClass;
	}

	@Override
	public Boolean getUnique() {
		return unique;
	}

	@Override
	public boolean getUnmodifiable() {
		return unmodifiable;
	}

	@Override
	public QueryMetadata setFetchPlan(String planAccesor) {
		this.planAccesor = planAccesor;
		return this;
	}

	@Override
	public QueryMetadata setLanguage(String language) {
		this.language = language;
		return null;
	}

	@Override
	public QueryMetadata setQuery(String query) {
		this.query = query;
		return this;
	}

	@Override
	public QueryMetadata setResultClass(String className) {
		this.resultClass = className;
		return this;
	}

	@Override
	public QueryMetadata setUnique(boolean unique) {
		this.unique = unique;
		return this;
	}

	@Override
	public QueryMetadata setUnmodifiable() {
		this.unmodifiable = true;
		return this;
	}

}
