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

import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;
import org.judal.serialization.BytesConverter;
import org.judal.storage.StorageObjectFactory;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.table.Record;

import com.knowgate.debug.DebugFile;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.SecondaryDatabase;
import com.sleepycat.db.SecondaryKeyCreator;

public class DBSecondaryIndexCreator implements SecondaryKeyCreator {

	private DBEntityBinding oDbeb;
	private TableDef oTbl;
	private Class<? extends Record> oRecCls;
	private String sIndx;
	private int iType;

	public DBSecondaryIndexCreator(DBEntityBinding oBind, String sBucketName, int iColumnType) {
		oDbeb = oBind;
		sIndx = "key";
		oTbl = DBBucketDataSource.getKeyValueDef(sBucketName);
		oRecCls = null;
		iType = iColumnType;
	}

	public DBSecondaryIndexCreator(DBEntityBinding oBind, Class<? extends Record> oRecClass, TableDef oTblDef, String sIndex, int iColumnType) {
		oDbeb = oBind;
		sIndx = sIndex;
		oTbl = oTblDef;
		oRecCls = oRecClass;
		iType = iColumnType;
	}

	@Override
	public boolean createSecondaryKey(SecondaryDatabase secDb, DatabaseEntry keyEntry, DatabaseEntry dataEntry, DatabaseEntry resultEntry) {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBSecondaryIndexCreator.createSecondaryKey(SecondaryDatabase, DatabaseEntry, DatabaseEntry, DatabaseEntry)");
			DebugFile.incIdent();
		}
		DBEntityWrapper oEnt = oDbeb.entryToObject(keyEntry,dataEntry);
		if (oRecCls==null) {
			Stored oSto = new DBStored(oTbl);
			oSto.setValue(oEnt.getWrapped());
			resultEntry.setData(BytesConverter.toBytes(oSto.getKey(), iType));
		} else {
			Record oRec;
			try {
				oRec = StorageObjectFactory.newRecord(oRecCls, oTbl);
				oRec.setValue(oEnt.getWrapped());
				byte [] data = oRec.isNull(sIndx) ? new byte[0] : BytesConverter.toBytes(oRec.apply(sIndx), iType);					
				if (DebugFile.trace) {
					if (oRec.isNull(sIndx)) {
						DebugFile.writeln("secondary key value is null");
					} else {
						DebugFile.writeln("key class is "+oRec.apply(sIndx).getClass().getName());
						DebugFile.writeln("key value is "+oRec.apply(sIndx));
						DebugFile.writeln("sql type "+ColumnDef.typeName(iType));
						DebugFile.writeln("key value is byte["+String.valueOf(data.length)+"]");
					}
				}
				resultEntry.setData(data);
			} catch (NoSuchMethodException n) { }
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBSecondaryIndexCreator.createSecondaryKey(SecondaryDatabase, DatabaseEntry, DatabaseEntry, DatabaseEntry)");
		}
		return true;
	} // createSecondaryKey

}
