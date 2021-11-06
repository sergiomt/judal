package org.judal.bdbj;

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

import javax.jdo.JDOException;

import org.judal.serialization.BytesConverter;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;

public class DBJSequenceGenerator implements javax.jdo.datastore.Sequence {

	private DBJDataSource oEnv;
	private String sName;
	private long lCurrent;

	public DBJSequenceGenerator(DBJDataSource dbEnvironment, String sequenceName) {
		oEnv = dbEnvironment;
		sName = sequenceName;
	}

	@Override
	public String getName() {
		return sName;
	}

	@Override
	public long nextValue() throws JDOException {
		SequenceConfig oQqg = new SequenceConfig();
		oQqg.setAutoCommitNoSync(true);
		oQqg.setAllowCreate(true);
		// oSqg.setRange(65536l,2147483647l);

		DatabaseEntry oKey = new DatabaseEntry(BytesConverter.toBytes(getName()));
		Database oQdb = null;
		Sequence oSqc = null;

		DatabaseConfig oSqg = new DatabaseConfig();
		oSqg.setTransactional(false);
		oSqg.setAllowCreate(true);
		oSqg.setReadOnly(false);

		try {
			oQdb = oEnv.getEnvironment().openDatabase(null, oEnv.getPath()+"Sequence.db", oSqg);
			oSqc = oQdb.openSequence(null, oKey, oQqg);
			lCurrent = oSqc.get(null, 1);
			oSqc.close();
			oSqc=null;
		} catch (IllegalArgumentException iae) {
			throw new JDOException("DBTable.nextVal("+getName()+") "+iae.getMessage(), iae);
		} catch (DatabaseException dbe) {
			throw new JDOException("DBTable.nextVal("+getName()+") "+dbe.getMessage(), dbe);
		} finally {
			if (null!=oSqc) { try { oSqc.close(); } catch (DatabaseException ignore) { } }
			if (null!=oQdb) { try { oQdb.close(); } catch (DatabaseException ignore) { } }
		}
		return lCurrent;
	} // nextValue

	@Override
	public void allocate(int arg0) {
	}

	@Override
	public Object current() {
		return lCurrent;
	}

	@Override
	public long currentValue() {
		return lCurrent;
	}

	@Override
	public Object next() {
		return nextValue();
	}
	
}