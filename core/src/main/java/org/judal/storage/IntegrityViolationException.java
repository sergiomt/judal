package org.judal.storage;

import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;

public class IntegrityViolationException extends JDOException {

  private static final long serialVersionUID = 70000l;

  public IntegrityViolationException (ColumnDef c, Object v) {
  	super("Integrity constraint violation. Value '"+(v==null ? "null" : v)+"' of column "+c.getName()+" does not match constraint "+c.getConstraint());
  }

  public IntegrityViolationException (String sMsg) {
  	super(sMsg);
  }

}
