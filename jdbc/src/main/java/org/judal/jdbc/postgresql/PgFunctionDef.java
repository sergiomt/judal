package org.judal.jdbc.postgresql;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.ProcedureDef;
import org.judal.metadata.Scriptable;
import org.judal.storage.Param;

public class PgFunctionDef extends ProcedureDef implements Scriptable {

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
