package org.judal.jdbc.postgresql;

import org.judal.metadata.Scriptable;
import org.judal.metadata.TriggerDef;

public class PgTriggerDef extends TriggerDef implements Scriptable {

	@Override
	public String getDrop() {
		return "DROP TRIGGER "+getName()+" ON "+getTable();
	}

}
