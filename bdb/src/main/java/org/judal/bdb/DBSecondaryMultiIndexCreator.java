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

import java.util.Collection;
import java.util.Set;

import org.judal.metadata.TableDef;
import org.judal.storage.StorageObjectFactory;
import org.judal.storage.table.Record;

import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.SecondaryDatabase;
import com.sleepycat.db.SecondaryMultiKeyCreator;

public class DBSecondaryMultiIndexCreator implements SecondaryMultiKeyCreator {

	private DBEntityBinding oDbeb;
	private String sIndx;
	private TableDef oTbl;
	private Class<? extends Record> oRecCls;


	public DBSecondaryMultiIndexCreator(DBEntityBinding oBind, Class<? extends Record> oRecClass, TableDef oTblDef, String sIndex) {
		oDbeb = oBind;
		sIndx = sIndex;
		oTbl = oTblDef;
		oRecCls = oRecClass;
	}

	@Override
	public void createSecondaryKeys(SecondaryDatabase secondary, DatabaseEntry keyEntry,DatabaseEntry dataEntry,Set results) throws DatabaseException {
		DBEntityWrapper oEnt = oDbeb.entryToObject(keyEntry,dataEntry);
		Record oRec;
		try {
			oRec = StorageObjectFactory.newRecord(oRecCls, oTbl);
			oRec.setValue(oEnt.getWrapped());
			Collection oFld = (Collection) oRec.apply(sIndx);
			if (null!=oFld) results.addAll(oFld);
		} catch (NoSuchMethodException neverthrown) { }
	}
}

