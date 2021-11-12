package org.judal.bdb;

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

import java.io.FileNotFoundException;

import javax.jdo.JDOException;

import org.judal.serialization.BytesConverter;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Sequence;
import com.sleepycat.db.SequenceConfig;

public class DBSequenceGenerator implements javax.jdo.datastore.Sequence {

	private DBDataSource oEnv;
	private String sName;
	private long lCurrent;

	public DBSequenceGenerator(DBDataSource dbEnvironment, String sequenceName) {
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
		oSqg.setType(DatabaseType.BTREE);

		try {
			oQdb = new Database(oEnv.getPath()+"Sequence.db", getName(), oSqg);
			oSqc = oQdb.openSequence(null, oKey, oQqg);
			lCurrent = oSqc.get(null, 1);
			oSqc.close();
			oSqc=null;
		} catch (FileNotFoundException fnf) {
			throw new JDOException("DBTable.nextVal("+getName()+") "+fnf.getMessage()+" "+oEnv.getPath()+"Sequence.db", fnf);
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
		return new Long(lCurrent);
	}

	@Override
	public long currentValue() {
		return lCurrent;
	}

	@Override
	public Object next() {
		return new Long(nextValue());
	}
	
}