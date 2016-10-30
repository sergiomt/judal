package org.judal.bdb;

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

import java.util.Iterator;

import javax.jdo.JDOException;

import org.judal.storage.Stored;

import com.knowgate.debug.DebugFile;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DeadlockException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;

public class DBIterator implements AutoCloseable, Iterator<Stored> {

	private Database oPdb;
	private StoredClassCatalog oCtg;
	private Cursor oCur = null;
	private DBEntityWrapper oNxt;
	private DBEntityBinding oDbeb;
	
	public DBIterator(Database oPdb, StoredClassCatalog oCtg) {
		this.oPdb = oPdb;
		this.oCtg = oCtg;
		this.oCur = null;
		this.oNxt = null;
		oDbeb = new DBEntityBinding(oCtg);
		open();
	}

	private void open() throws JDOException {
		DatabaseEntry oDbKey = new DatabaseEntry();
		DatabaseEntry oDbDat = new DatabaseEntry();
		OperationStatus oOst;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBIterator.open()");
			DebugFile.incIdent();
		}

		try {
			if (DebugFile.trace) DebugFile.writeln("Database.openCursor(null,null)");
			
			oCur = oPdb.openCursor(null,null);

			oOst = oCur.getFirst(oDbKey, oDbDat, LockMode.DEFAULT);
			if (oOst == OperationStatus.SUCCESS)
				oNxt = oDbeb.entryToObject(oDbKey,oDbDat);
			else
				oNxt = null;

		} catch (DeadlockException dlxc) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new JDOException(dlxc.getMessage(), dlxc);
		} catch (DatabaseException xcpt) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBIterator.open()");
		}

	} // open
	
	@Override
	public boolean hasNext() {
		DatabaseEntry oDbKey = new DatabaseEntry();
		DatabaseEntry oDbDat = new DatabaseEntry();
		OperationStatus oOst;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBIterator.hasNext()");
			DebugFile.incIdent();
		}

		try {
			if (DebugFile.trace) DebugFile.writeln("Database.openCursor(null,null)");
			
			oOst = oCur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);
			if (oOst == OperationStatus.SUCCESS)
				oNxt = oDbeb.entryToObject(oDbKey,oDbDat);
			else
				oNxt = null;

		} catch (DeadlockException dlxc) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new JDOException(dlxc.getMessage(), dlxc);
		} catch (DatabaseException xcpt) {
			if (DebugFile.trace) DebugFile.decIdent();
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBIterator.hasNext()");
		}
		return oNxt!=null;
	}

	@Override
	public Stored next() {
		return (Stored) oNxt.getWrapped();
	}

	@Override
	public void close() {
		try { if (oCur!=null) oCur.close(); } catch (Exception ignore) { }
	}

}
