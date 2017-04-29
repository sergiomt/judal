package org.judal.ramqueue;

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

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import javax.jdo.JDOException;
import javax.jms.JMSException;

import org.judal.storage.Engine;
import org.judal.storage.EngineFactory;
import org.judal.storage.DataSource;
import org.judal.storage.Param;
import org.judal.storage.queue.RecordQueueConsumer;
import org.judal.storage.queue.RecordQueueProducer;
import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.Record;
import org.judal.storage.table.Table;
import org.judal.storage.table.TableDataSource;

import com.knowgate.debug.DebugFile;
import com.knowgate.debug.StackTraceUtil;
import com.knowgate.tuples.Pair;
import com.knowgate.tuples.Triplet;

import org.judal.jms.ObjectMessageImpl;

/**
 * RecordQueueProducer interface implementation
 * @author Sergio Montoro Ten
 */
public class RAMQueueProducer implements RecordQueueProducer {

	private AtomicLong oMsgId;
	private boolean bGenerateIds;
	private RAMQueueConsumer oCon;
	private TableDataSource oDts;

	public RAMQueueProducer(String engineName, Map<String,String> properties)
			throws JDOException, IllegalStateException, InstantiationException {
		if (DebugFile.trace) DebugFile.writeln("new RAMQueueConsumer("+this+")");
		bGenerateIds = false;
		if (bGenerateIds) oMsgId = new AtomicLong(0l);
		oCon = new RAMQueueConsumer();
		Engine<? extends DataSource> oEng = null;
		try {
			oEng = EngineFactory.getEngine(engineName);
			oCon.start(oEng, properties);
			oDts = (TableDataSource) oEng.getDataSource(properties);
		} catch (NullPointerException | IllegalAccessException xcpt) {
			try { if (DebugFile.trace) DebugFile.writeln(com.knowgate.debug.StackTraceUtil.getStackTrace(xcpt)); } catch (Exception ignore) { }
			throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
		}
	}

	public RecordQueueConsumer consumer() {
		return oCon;
	}

	@Override
	public void close() throws JDOException {

		if (DebugFile.trace) {
			DebugFile.writeln("Begin RAMQueueProducer.close()");
			DebugFile.incIdent();
		}

		oCon.stop();
		oCon = null;

		if (oDts!=null) {
			oDts.close();
			oDts=null;
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End RAMQueueProducer.close()");
		}
	}

	@Override
	public void insert(Record oRec, Param[] aParams) throws JDOException {
		if (oCon==null) throw new IllegalStateException("Queue is closed");
		ObjectMessageImpl oMsg = new ObjectMessageImpl(oDts);
		try {
			oMsg.setObject(new Pair<Record,Param[]>(oRec, aParams));
			if (bGenerateIds) oMsg.setJMSMessageID(String.valueOf(oMsgId.addAndGet(1l)));
			oMsg.setJMSTimestamp(System.currentTimeMillis());
			oMsg.setIntProperty("command", ObjectMessageImpl.COMMAND_INSERT_RECORD);
		} catch (JMSException neverthrown) { }
		oCon.onMessage(oMsg);
	}

	@Override
	public void insert(Record oRec, Param[] aParams, Properties oProps) throws JDOException {
		if (oProps.getProperty("synchronous","false").equals("true")) {
			IndexableTable oTbl = null;
			try {
			  oTbl = oDts.openIndexedTable(oRec);
			  oTbl.insert(aParams);
			} finally {
				if (oTbl!=null) oTbl.close();
			}
		} else {
			insert(oRec, aParams);
		}
	}

	@Override
	public void update(Record oRec, Param[] aParams, Param[] aWhere) throws JDOException {
		if (oCon==null) throw new IllegalStateException("Queue is closed");
		ObjectMessageImpl oMsg = new ObjectMessageImpl(oDts);
		try {
			oMsg.setObject(new Triplet<Record,Param[],Param[]>(oRec, aParams, aWhere));
			if (bGenerateIds) oMsg.setJMSMessageID(String.valueOf(oMsgId.addAndGet(1l)));
			oMsg.setJMSTimestamp(System.currentTimeMillis());
			oMsg.setIntProperty("command", ObjectMessageImpl.COMMAND_UPDATE_RECORD);
		} catch (JMSException neverthrown) { }
		oCon.onMessage(oMsg);
	}

	@Override
	public void update(Record oRec, Param[] aParams, Param[] aWhere, Properties oProps) throws JDOException {
		if (oProps.getProperty("synchronous","false").equals("true")) {
			IndexableTable oTbl = null;
			try {
			  oTbl = oDts.openIndexedTable(oRec);
			  oTbl.update(aParams, aWhere);
			} finally {
				if (oTbl!=null) oTbl.close();
			}
		} else {
			update(oRec, aParams, aWhere);
		}
	}
	
	@Override
	public void store(Record oRec) throws JDOException {
		if (oCon==null) throw new IllegalStateException("Queue is closed");
		ObjectMessageImpl oMsg = new ObjectMessageImpl(oDts);
		try {
			oMsg.setObject(oRec);
			if (bGenerateIds) oMsg.setJMSMessageID(String.valueOf(oMsgId.addAndGet(1l)));
			oMsg.setJMSTimestamp(System.currentTimeMillis());
			oMsg.setIntProperty("command", ObjectMessageImpl.COMMAND_STORE_RECORD);
		} catch (JMSException neverthrown) { }
		oCon.onMessage(oMsg);
	}

	@Override
	public void store(Record[] aRecs) throws JDOException, ArrayIndexOutOfBoundsException {
		if (oCon==null) throw new IllegalStateException("Queue is closed");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin RAMQueueProducer.store(Record["+String.valueOf(aRecs.length)+"])");
			DebugFile.incIdent();
		}

		ObjectMessageImpl oMsg = new ObjectMessageImpl(oDts);
		try {
			oMsg.setObject(aRecs);
			if (bGenerateIds) oMsg.setJMSMessageID(String.valueOf(oMsgId.addAndGet(1l)));
			oMsg.setJMSTimestamp(System.currentTimeMillis());
			oMsg.setIntProperty("command", ObjectMessageImpl.COMMAND_STORE_RECORD);
		} catch (JMSException neverthrown) { }

		oCon.onMessage(oMsg);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End RAMQueueProducer.store(Record["+String.valueOf(aRecs.length)+"])");
		}
	}

	@Override
	public void store(Record[] aRecs, Properties oProps) throws JDOException {
		if (oCon==null) throw new IllegalStateException("Queue is closed");

		if (DebugFile.trace) {
			DebugFile.writeln("Begin RAMQueueProducer.store(Record["+String.valueOf(aRecs.length)+"])");
			DebugFile.incIdent();
		}

		if (oProps.getProperty("synchronous","false").equals("true")) {
			Table oTbl = null;
			final int nRecs=aRecs.length;
			for (int r=0; r<nRecs; r++) {
				try {
					Record oRec = aRecs[r];
					if (null==oTbl)
						oTbl = oDts.openTable(oRec);
					oTbl.store(oRec);
					if (r==nRecs-1) {
						oTbl.close();
						oTbl = null;  
					} else if (!oRec.getTableName().equals(aRecs[r+1].getTableName())) {
						oTbl.close();
						oTbl = null;  
					}
				} catch (Exception xcpt) {
					if (DebugFile.trace) {
						DebugFile.writeln("RAMQueueProducer.store() "+xcpt.getClass().getName()+" "+xcpt.getMessage());
						try {
							DebugFile.writeln(StackTraceUtil.getStackTrace(xcpt));
						} catch (IOException ignore) { }
					}				
					try {
						if (oTbl!=null) oTbl.close();
					} catch (Exception ignore) { }
					oTbl=null;
				} 			  
			}

		} else {

			ObjectMessageImpl oMsg = new ObjectMessageImpl(oDts);
			try {
				oMsg.setObject(aRecs);
				if (bGenerateIds) oMsg.setJMSMessageID(String.valueOf(oMsgId.addAndGet(1l)));
				oMsg.setJMSTimestamp(System.currentTimeMillis());
				oMsg.setIntProperty("command", ObjectMessageImpl.COMMAND_STORE_RECORD);
			} catch (JMSException neverthrown) { }
			oCon.onMessage(oMsg);
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End RAMQueueProducer.store(Record["+String.valueOf(aRecs.length)+"])");
		}
	}

	public void store(Record oRec, Properties oProps) throws JDOException {
		if (oCon==null) throw new IllegalStateException("Queue is closed");
		if (oProps.getProperty("synchronous","false").equals("true")) {
			IndexableTable oTbl = null;
			try {
				oTbl = oDts.openIndexedTable(oRec);
				oTbl.store(oRec);
				oTbl.close();
				oTbl = null;
			} catch (Exception xcpt) {
				if (DebugFile.trace) {
					DebugFile.writeln("RAMQueueProducer.store() "+xcpt.getClass().getName()+" "+xcpt.getMessage());
					try {
						DebugFile.writeln(StackTraceUtil.getStackTrace(xcpt));
					} catch (IOException ignore) { }
				}				

			} finally {
				try {
					if (oTbl!=null) oTbl.close();
				} catch (Exception xcpt) { }
			}			

		} else {

			ObjectMessageImpl oMsg = new ObjectMessageImpl(oDts);
			try {
				oMsg.setObject(oRec);
				if (bGenerateIds) oMsg.setJMSMessageID(String.valueOf(oMsgId.addAndGet(1l)));
				oMsg.setJMSTimestamp(System.currentTimeMillis());
				oMsg.setIntProperty("command", ObjectMessageImpl.COMMAND_STORE_RECORD);
			} catch (JMSException neverthrown) { }
			oCon.onMessage(oMsg);

		}
	}

	@Override
	public void delete(Record oRec, String[] aKeys, Properties oProps) throws JDOException {
		if (oCon==null) throw new IllegalStateException("Queue is closed");
		final boolean bSync = oProps.getProperty("synchronous","false").equals("true");
		if (DebugFile.trace) {
			String sKeys = "[";
			for (String k : aKeys) sKeys += (sKeys.length()==1 ? "" : ",") + k;
			DebugFile.writeln("Begin RAMQueueProducer.delete(Record, "+sKeys+"], synchronous="+String.valueOf(bSync)+")");
			DebugFile.incIdent();
		}
		if (bSync) {
			Table oTbl = null;
			try {
				oTbl = oDts.openTable(oRec);
				for (String k : aKeys) {
					oRec.setKey(k);
					oTbl.delete(oRec);
				} // next
				oTbl.close();
				oTbl = null;
			} catch (Exception xcpt) {
				if (DebugFile.trace) {
					DebugFile.writeln("RAMQueueProducer.delete() "+xcpt.getClass().getName()+" "+xcpt.getMessage());
					try {
						DebugFile.writeln(StackTraceUtil.getStackTrace(xcpt));
					} catch (IOException ignore) { }
				}
			} finally {
				try {
					if (oTbl!=null) oTbl.close();
				} catch (Exception xcpt) { }
			}			

		} else {

			ObjectMessageImpl oMsg = new ObjectMessageImpl(oDts);
			try {
				oMsg.setObject(oRec);
				if (bGenerateIds) oMsg.setJMSMessageID(String.valueOf(oMsgId.addAndGet(1l)));
				oMsg.setJMSTimestamp(System.currentTimeMillis());
				oMsg.setIntProperty("command", ObjectMessageImpl.COMMAND_DELETE_RECORDS);
				boolean first = false;
				StringBuffer buff = new StringBuffer();
				for (String k : aKeys) {
					if (first)
						first = false;
					else
						buff.append("`");
					buff.append(k);					
				}
				oMsg.setStringProperty("keys", buff.toString());
			} catch (JMSException neverthrown) { }			
			oCon.onMessage(oMsg);

		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End RAMQueueProducer.delete(Record. String[])");
		}		
	}

	@Override
	public void stop(boolean bInmediate, int iTimeout) throws JDOException {
		if (iTimeout>0 && !bInmediate) {
			try {
				Thread.sleep(iTimeout);
			} catch (InterruptedException e) { }			
		}
		close();
	}

}