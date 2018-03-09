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
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

import javax.jdo.JDOException;

import org.judal.metadata.TableDef;
import org.judal.storage.keyvalue.Stored;

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

	private String sTbl;
	private Database oPdb;
	private StoredClassCatalog oCtg;
	private Cursor oCur = null;
	private Queue<Stored> oNxt;
	private DBEntityBinding oDbeb;
	private boolean isFirst;

	public DBIterator(String sTbl, Database oPdb, StoredClassCatalog oCtg) {
		this.sTbl = sTbl;
		this.oPdb = oPdb;
		this.oCtg = oCtg;
		this.oCur = null;
		this.oNxt = new LinkedList<Stored>();
		oDbeb = new DBEntityBinding(oCtg);
		open();
	}

	private void open() throws JDOException {

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBIterator.open()");
			DebugFile.incIdent();
		}

		isFirst = true;

		try {
			if (DebugFile.trace) DebugFile.writeln("Database.openCursor(null,null)");

			oCur = oPdb.openCursor(null,null);

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
		boolean nextFound;
		if (oNxt.isEmpty()) {
			try {
				oNxt.add(next());
				nextFound = true;
			} catch (NoSuchElementException nonext) {
				nextFound =  false;
			}
		} else {
			nextFound = true;
		}
		return nextFound;
	}


	@Override
	public Stored next() throws NoSuchElementException {
		Stored oNext;
		DatabaseEntry oDbKey = new DatabaseEntry();
		DatabaseEntry oDbDat = new DatabaseEntry();
		DBEntityWrapper oDbWrp;
		OperationStatus oOst;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBIterator.hasNext()");
			DebugFile.incIdent();
		}

		oNext = oNxt.poll();
		if (null==oNext) {
			try {
				if (DebugFile.trace) DebugFile.writeln("Database.openCursor(null,null)");

				if (isFirst) {
					oOst = oCur.getFirst(oDbKey, oDbDat, LockMode.DEFAULT);
					isFirst = false;
				} else {
					oOst = oCur.getNext(oDbKey, oDbDat, LockMode.DEFAULT);
				}

				if (oOst == OperationStatus.SUCCESS) {
					oDbWrp = oDbeb.entryToObject(oDbKey,oDbDat);
					oNext = new DBStored(new TableDef(sTbl));
					oNext.setKey(oDbWrp .getKey());
					oNext.setValue(oDbWrp .getWrapped());
				} else {
					oNext = null;
				}

			} catch (DeadlockException dlxc) {
				if (DebugFile.trace) DebugFile.decIdent();
				throw new JDOException(dlxc.getMessage(), dlxc);
			} catch (DatabaseException xcpt) {
				if (DebugFile.trace) DebugFile.decIdent();
				throw new JDOException(xcpt.getMessage(), xcpt);
			}

		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBIterator.hasNext()");
		}

		if (null==oNext) {
			throw new NoSuchElementException();
		}
		return oNext;
	}

	@Override
	public void close() {
		oNxt.clear();
		try { if (oCur!=null) oCur.close(); } catch (Exception ignore) { }
	}

}
