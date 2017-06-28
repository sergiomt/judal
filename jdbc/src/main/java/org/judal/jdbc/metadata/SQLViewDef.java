package org.judal.jdbc.metadata;

import javax.jdo.JDOUserException;

import org.judal.metadata.Scriptable;
import org.judal.metadata.ViewDef;
import org.judal.storage.Param;

public class SQLViewDef extends ViewDef implements Scriptable {

	private static final long serialVersionUID = 1L;
	
	private String viewSource;
	
	public SQLViewDef(String aliasedName, String viewSource) throws JDOUserException {
		super(aliasedName);
		this.viewSource = viewSource;
	}

	public String getSource() {
		return viewSource;
	}

	public String getDrop() {
		return "DROP VIEW "+getName();
	}

	@Override
	public Param[] getParams() {
		return new Param[0];
	}
	
}
