package org.judal.metadata;

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
	
	/**
	 * <p>Constructor.</p>
	 * Set action and when as UNDEFINED
	 */
	public TriggerDef() {
		action = Action.UNDEFINED;
		when = When.UNDEFINED;
	}
	
	/**
	 * @param tableName String
	 */
	public void setTable(String tableName) {
		table = tableName;
	}

	/**
	 * @return String Table name
	 */
	public String getTable() {
		return table;
	}

	/**
	 * @param procedure ProcedureDef
	 */
	public void setProcedure(ProcedureDef procedure) {
		this.proc = procedure;
	}

	/**
	 * @return ProcedureDef
	 */
	public ProcedureDef getProcedure() {
		return proc;
	}

	/**
	 * @return Action INSERT, UPDATE, DELETE  or UNDEFINED
	 */
	public Action getAction() {
		return action;
	}

	/**
	 * @param action Action INSERT, UPDATE, DELETE or UNDEFINED
	 */
	public void setAction(Action action) {
		this.action = action;
	}

	/**
	 * @return When BEFORE or AFTER action
	 */
	public When getWhen() {
		return when;
	}

	/**
	 * @param when When BEFORE or AFTER action
	 */
	public void setWhen(When when) {
		this.when = when;
	}
	
}
