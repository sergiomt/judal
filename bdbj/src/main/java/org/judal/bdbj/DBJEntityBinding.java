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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.util.FastInputStream;
import com.sleepycat.util.FastOutputStream;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.serial.SerialSerialBinding;

import com.knowgate.debug.DebugFile;
import com.knowgate.debug.StackTraceUtil;

class DBJEntityBinding extends SerialSerialBinding<byte[],byte[],DBEntityWrapper> {

	private static final Class CKEY = new byte[0].getClass();
	private static final Class CDATA = new byte[1].getClass();
	
	public DBJEntityBinding(ClassCatalog oCtg, Class<byte[]> cKey, Class<byte[]> cDat) {
		super(oCtg,cKey,cDat);
	} 

	public DBJEntityBinding(SerialBinding<byte[]> oKey, SerialBinding<byte[]> oDat) {
		super(oKey,oDat);
	}

	public DBJEntityBinding(StoredClassCatalog oCtg) {
		super((ClassCatalog) oCtg, CKEY, CDATA);
	}

	@Override
	public DBEntityWrapper entryToObject(byte[] byKey, byte[] aBytes) {
  		if (DebugFile.trace) {
  			DebugFile.writeln("Begin DBEntityBinding.entryToObject(byte[],byte[])");
  			DebugFile.incIdent();
  		}
		DBEntityWrapper oEnt = null;
		try {
			FastInputStream oByIn = new FastInputStream(aBytes);
  			ObjectInputStream oObIn = new ObjectInputStream(oByIn);
  			oEnt = (DBEntityWrapper) oObIn.readObject();
  			oObIn.close();
  			oByIn.close();
		} catch (IOException xcpt) {
			String s = "";
			try { s = StackTraceUtil.getStackTrace(xcpt); } catch (Exception x) {}
			if (DebugFile.trace) DebugFile.writeln("entryToObject("+byKey+", byte[]) IOException "+xcpt.getMessage()+" "+s);
		} catch (ClassNotFoundException xcpt) {
	 	  if (DebugFile.trace) DebugFile.writeln("entryToObject("+byKey+", byte[]) ClassNotFoundException "+xcpt.getMessage());
		}
  		if (DebugFile.trace) {
  			DebugFile.decIdent();
  			DebugFile.writeln("End DBEntityBinding.entryToObject()");
  		}
  		return oEnt;
	}
	
	public byte[] objectToKey(DBEntityWrapper oEnt) {
		return oEnt.getKey();
	}

	@Override
	public byte[] objectToData(DBEntityWrapper oEnt) {
		byte[] aBytes = null;
		try {
			FastOutputStream oByOut = new FastOutputStream(4000);
  			ObjectOutputStream oObOut = new ObjectOutputStream(oByOut);
  			oObOut.writeObject(oEnt);		
	  		aBytes = oByOut.toByteArray();
	  		oObOut.close();
			oByOut.close();
		} catch (IOException xcpt) {
			String s = "";
			try { s = StackTraceUtil.getStackTrace(xcpt); } catch (Exception x) {}
			if (DebugFile.trace) DebugFile.writeln("IOException "+xcpt.getMessage()+"\n"+s);
		}
		return aBytes;
  	}	
  	
  	public DBEntityWrapper entryToObject(DatabaseEntry oKey, DatabaseEntry oDat) {
  		if (DebugFile.trace) {
  			DebugFile.writeln("Begin DBEntityBinding.entryToObject(DatabaseEntry,DatabaseEntry)");
  			DebugFile.incIdent();
  		}
  		DBEntityWrapper retval;
  		if (null==oKey)
  			retval = entryToObject(null, oDat.getData());
  		else if (null==oKey.getData())
  			retval = entryToObject(null, oDat.getData());
  		else
  			retval = entryToObject(oKey.getData(), oDat.getData());
  		if (DebugFile.trace) {
  			DebugFile.decIdent();
  			DebugFile.writeln("End DBEntityBinding.entryToObject(DatabaseEntry,DatabaseEntry)");
  		}
  		return retval;
  	}

}