package org.judal.storage;

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

import java.sql.Types;
import java.util.Arrays;
import java.util.Date;

import javax.jdo.JDOException;
import javax.jdo.datastore.Sequence;

import org.judal.metadata.ColumnDef;
import org.judal.storage.table.Record;

import com.knowgate.debug.DebugFile;
import com.knowgate.stringutils.Slugs;
import com.knowgate.stringutils.Uid;

public class StandardConstraintsChecker implements ConstraintsChecker {

	private Sequence sequencer;
	private ForeignKeyChecker fkChecker;
	
	// --------------------------------------------------------------------------

	/**
	 * <p>Constructor</p>
	 * @param sequencer Sequence
	 * @param fkChecker ForeignKeyChecker
	 */
	public StandardConstraintsChecker(Sequence sequencer, ForeignKeyChecker fkChecker) {
		this.sequencer = sequencer;		
		this.fkChecker = fkChecker;
	}
	
	// --------------------------------------------------------------------------

	/**
	 * <p>Perform validations on a Record:</p>
	 * <ol>
	 * <li>For each column which value is <b>null</b> and has a default value defined, set the default value.</li>
	 * <li>After performing step 1, check that columns with a NOT NULL constraint actually do not contain a <b>null</b> value.</li>
	 * <li>Use the provided ForeignKeyChecker (if any) to check foreign key constraints.</li>
	 * </ol>
	 * @param oDts DataSource
	 * @param oRec Rec
	 * @throws JDOException
	 */
	public void check(DataSource oDts, Record oRec)
			throws JDOException {

		if (null!=oRec.columns()) {

			boolean bHasCompoundIndexes = false;

			if (DebugFile.trace) {
				String sColNames = "Iterating through";
				for (ColumnDef c : oRec.columns()) sColNames += " "+c.getName();
				DebugFile.writeln(sColNames);
			}

			for (ColumnDef c : oRec.columns()) {
				String n = c.getName();

				boolean bIsEmptyPk = (oRec.getColumn(n)==null);
				if (!bIsEmptyPk) {
					Object oPk = oRec.apply(n);
					bIsEmptyPk = (oPk==null);
					if (!bIsEmptyPk) bIsEmptyPk = (oPk.toString().length()==0);
				}

				Object sDefVal = c.getDefaultValue();

				if (sDefVal!=null) {
					if (bIsEmptyPk) {

						if (sDefVal.equals("GUID")) {

							oRec.put(n, Uid.createUniqueKey());

						} else if (sDefVal.equals("SERIAL")) {
							String sSerial = String.valueOf(sequencer.nextValue());
							int iPadLen = (c.getType()==Types.BIGINT ? 21 : 11) - sSerial.length();
							if (iPadLen>0) {
								char aPad[] = new char[iPadLen];
								Arrays.fill(aPad, '0');
								sSerial = new String(aPad) + sSerial;
							}
							oRec.put(n, sSerial);

						} else if (sDefVal.equals("NOW")) {
							
							oRec.put(n, new Date());							  	

						} else if (sDefVal.toString().indexOf('+')<0) {

							if (sDefVal.toString().startsWith("$"))
								oRec.put(n, Slugs.transliterate(oRec.getString(sDefVal.toString().substring(1))).replace(' ','_').toLowerCase());
							else if (sDefVal.toString().startsWith("'"))
								oRec.put(n, sDefVal.toString().substring(1, sDefVal.toString().indexOf((char) 39, 1)));
							else
								oRec.put(n, sDefVal);
						} else {
							String[] aCols = sDefVal.toString().split("\\x2B");
							sDefVal = "";
							if (aCols!=null)
								for (int v=0; v<aCols.length; v++)
									if (aCols[v].startsWith("$"))
										sDefVal = sDefVal + Slugs.transliterate(oRec.getString(aCols[v].substring(1))).replace(' ','_').toLowerCase();
									else if (aCols[v].startsWith("'"))
										sDefVal = sDefVal + aCols[v].substring(1, aCols[v].indexOf((char) 39, 1));
									else
										sDefVal = sDefVal.toString() + oRec.apply(aCols[v]);
							oRec.put(n, sDefVal);				
						}
						if (c.isPrimaryKey()) {
							if (DebugFile.trace)
								DebugFile.writeln("auto setting default value of "+n+" to "+oRec.apply(n));
							oRec.setKey(oRec.apply(n).toString());
						}
					} // fi
					bHasCompoundIndexes = (sDefVal.toString().indexOf('+')>0);
				} // fi

				if (!c.getAllowsNull() && (oRec.getColumn(n)==null)) {
					if (sDefVal==null)
						throw new IntegrityViolationException(c, null);
					else if (sDefVal.toString().indexOf('+')<0)
						throw new IntegrityViolationException(c, null);
				}

				if (fkChecker!=null && oRec.getColumn(n)!=null && c.getTarget()!=null && c.getTargetField()!=null) {
					if (oRec.apply(n)!=null) {
						if (oRec.apply(n).toString().length()>0) {
							if (DebugFile.trace)
								DebugFile.writeln("Checking "+c.getTarget()+"."+c.getTargetField()+"."+c.getName()+" for value "+oRec.apply(n));
							boolean bExists = fkChecker.exists(c.getTarget(), c.getName(), oRec.apply(n));
							if (!bExists) throw new IntegrityViolationException(c,oRec.apply(n));
						} // fi
					} // fi ()
				} // fi (c.getForeignKey())

				if (c.getType()==Types.BOOLEAN) {
					if (oRec.getColumn(n)!=null) {
						if (oRec.apply(n) instanceof String) {
							String s = (String) oRec.apply(n);
							if (s.equals("true") || s.equals("True") || s.equals("TRUE") ||
								s.equals("yes")  || s.equals("Yes")  || s.equals("YES")  ||
								s.equals("1"))
								oRec.replace(n, Boolean.TRUE);
							else if (s.equals("false") || s.equals("False") || s.equals("FALSE") ||
									 s.equals("no")  || s.equals("No")  || s.equals("NO")  ||
									s.equals("0"))
								oRec.replace(n, Boolean.FALSE);
						} // fi (instanceof String)
						if (oRec.apply(n) instanceof Short) {
							if (oRec.apply(n).equals(new Short((short)1)))
								oRec.replace(n, Boolean.TRUE);
							else if (oRec.apply(n).equals(new Short((short)0)))
								oRec.replace(n, Boolean.FALSE);
						} // fi (instanceof Short)
						if (oRec.apply(n) instanceof Integer) {
							if (oRec.apply(n).equals(new Integer(1)))
								oRec.replace(n, Boolean.TRUE);
							else if (oRec.apply(n).equals(new Integer(0)))
								oRec.replace(n, Boolean.FALSE);
						} // fi (instanceof Integer)  	        
						if (!(oRec.apply(n) instanceof Boolean)) {
							throw new IntegrityViolationException(c,"Must be of type Boolean but is actually "+oRec.apply(n).getClass().getName());
						}
					}
				} // fi (BOOLEAN)
			} // next

			if (DebugFile.trace) {
				DebugFile.writeln("table "+(bHasCompoundIndexes ? " has " : " has not ")+"compound indexes");  	    	
			}

			if (bHasCompoundIndexes) {
				for (ColumnDef c : oRec.columns()) {
					String n = c.getName();
					if (c.getDefaultValue()!=null) {
						String sDefVal = c.getDefaultValue().toString();
						if (sDefVal.indexOf('+')>0) {
							String[] aIndexColumns = sDefVal.split("\\x2B");
							StringBuffer oCompoundIndexValue = new StringBuffer(1000);
							for (int i=0; i<aIndexColumns.length; i++) {
								oCompoundIndexValue.append(oRec.apply(aIndexColumns[i]));
							} // next
							oRec.put(n, oCompoundIndexValue.toString());
							if (DebugFile.trace) {
								DebugFile.writeln("compound index "+n+" value set to \""+oRec.getString(n)+"\"");
							}
							// } // fi
						} // fi (indexOf('+')>0)
					} // fi (getDefaultValue())
					if (!c.check(oRec.apply(n)))
						throw new IntegrityViolationException(c, oRec.apply(n));
				} // next (c)
			} else {
				for (ColumnDef c : oRec.columns()) {
					if (!c.check(oRec.apply(c.getName())))
						throw new IntegrityViolationException(c, oRec.apply(c.getName()));
				} // next
			}
		} // fi

		boolean hasPrimaryKey = false;
		for (ColumnDef cdef : oRec.columns()) {
			hasPrimaryKey = cdef.isPrimaryKey();
			if (hasPrimaryKey) break;
		}

		if (hasPrimaryKey) {
			if (oRec.getKey()==null)
				throw new IntegrityViolationException("Primary key not set and no default specified at table "+oRec.getTableName());
			else if (oRec.getKey().toString().length()==0)
				throw new IntegrityViolationException("Empty string not allowed as primary key at table "+oRec.getTableName());
		}
		
	} // checkConstraints
	
}
