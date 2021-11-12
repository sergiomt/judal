package org.judal.jdbc.postgresql;

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

import org.judal.metadata.ColumnDef;
import org.judal.metadata.ProcedureDef;
import org.judal.metadata.Scriptable;
import org.judal.storage.Param;

public class PgFunctionDef extends ProcedureDef implements Scriptable {

	private static final long serialVersionUID = 1L;

	@Override
	public String getDrop() {
		StringBuilder dropSql = new StringBuilder();
		dropSql.append("DROP FUNCTION ").append(getName()).append("(");
		Param[] params = getParams();
		for (int p=0; p<params.length; p++)
			dropSql.append(p==0 ? "" : ",").append(ColumnDef.typeName(params[p].getType()));
		dropSql.append(")");
		return dropSql.toString();
	}
	
}
