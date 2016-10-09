package org.judal.jdbc.metadata;

import javax.jdo.JDOUserException;

import org.judal.metadata.ViewDef;

public class SQLViewDef extends ViewDef {

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
	
}
