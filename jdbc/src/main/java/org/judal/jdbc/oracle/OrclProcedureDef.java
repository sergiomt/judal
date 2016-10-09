package org.judal.jdbc.oracle;

import org.judal.metadata.ProcedureDef;
import org.judal.metadata.Scriptable;

public class OrclProcedureDef extends ProcedureDef implements Scriptable {

	@Override
	public String getDrop() {
		return "DROP PROCEDURE "+getName();
	}

}
