package org.judal.jdbc.oracle;

import org.judal.metadata.Scriptable;
import org.judal.metadata.SequenceDef;
import org.judal.storage.Param;

public class OrclSequenceDef extends SequenceDef implements Scriptable {

	@Override
	public String getSource() {
		return "CREATE SEQUENCE "+getName()+" INCREMENT BY 1 START WITH "+String.valueOf(getInitialValue());
	}

	@Override
	public Param[] getParams() {
		return new Param[0];
	}

	@Override
	public String getDrop() {
		return "DROP SEQUENCE "+getName();
	}

}
