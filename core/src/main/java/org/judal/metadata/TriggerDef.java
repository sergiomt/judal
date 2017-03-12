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

/**
 * Definition of a trigger
 * @author Sergio Montoro Ten
 */
public class TriggerDef extends CallableDef {

	private static final long serialVersionUID = 10000l;

	public enum Action {
		INSERT, UPDATE, DELETE, UNDEFINED;
	}

	public enum When {
		BEFORE, AFTER, UNDEFINED;
	}

	private String table;
	private Action action;
	private When when;
	private ProcedureDef proc;
	
	public TriggerDef() {
		action = Action.UNDEFINED;
		when = When.UNDEFINED;
	}
	
	public void setTable(String tableName) {
		table = tableName;
	}

	public String getTable() {
		return table;
	}

	public void setProcedure(ProcedureDef procedure) {
		this.proc = procedure;
	}

	public ProcedureDef getProcedure() {
		return proc;
	}

	public Action getAction() {
		return action;
	}

	public void setAction(Action action) {
		this.action = action;
	}

	public When getWhen() {
		return when;
	}

	public void setWhen(When when) {
		this.when = when;
	}
	
}
