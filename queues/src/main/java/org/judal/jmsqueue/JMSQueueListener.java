package org.judal.jmsqueue;

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

import java.util.Enumeration;
import java.util.Map;

import javax.jdo.JDOException;

import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.transaction.TransactionManager;
import javax.jms.ObjectMessage;
import javax.jms.TemporaryQueue;
import javax.jms.MessageProducer;
import javax.jms.MessageListener;

import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.Record;
import org.judal.storage.table.Table;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.Param;
import org.judal.storage.EngineFactory;

import com.knowgate.debug.DebugFile;
import com.knowgate.tuples.Pair;
import com.knowgate.tuples.Triplet;

public class JMSQueueListener implements MessageListener {

	@SuppressWarnings("unused")
	private Map<String,String> properties;
	private Session sssn;
	private TableDataSource dts;

	/**
	 * <p>Constructor.</p>
	 * Call EngineFactory.getEngine(engineName).getDataSource(properties, transactManager) to create a DataSource instance.
	 * @param engineName String
	 * @param properties Map&lt;String,String&gt;
	 * @param transactManager TransactionManager
	 * @param sssn Session
	 * @throws JDOException
	 */
	public JMSQueueListener(String engineName, Map<String,String> properties, TransactionManager transactManager, Session sssn) throws JDOException {
		this.properties = properties;
		this.sssn = sssn;
		try {
			dts = (TableDataSource) EngineFactory.getEngine(engineName).getDataSource(properties, transactManager);
		} catch (NullPointerException | InstantiationException | IllegalAccessException | ClassCastException xcpt) {
			try { if (DebugFile.trace) DebugFile.writeln(com.knowgate.debug.StackTraceUtil.getStackTrace(xcpt)); } catch (Exception ignore) { }
			throw new JDOException(xcpt.getClass().getName()+" "+xcpt.getMessage(), xcpt);
		}	  
	}

	/**
	 * <p>Close the DataSource used by thisMessageListener.</p>
	 * @throws JDOException
	 */
	public void close() throws JDOException {
		if (dts!=null) {
			dts.close();
			dts=null;
		}
	}

	/**
	 * <p>Process message.</p>
	 * Read "command" int property from Message.
	 * If Message is instance of ObjectMessage Then
	 * Depending on the value of "command" property
	 * insert, update, store or delete records.
	 * If Message.getJMSReplyTo() is not null And Session is not null Then
	 * send a TextMessage as reply.
	 * @param oMsg Message
	 */
	public void onMessage (Message oMsg) {

		IndexableTable itbl = null;
		TemporaryQueue rpl;
		MessageProducer rpr = null;
		String serr = null; 

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JMSQueueListener.onMessage([Message])");
			DebugFile.incIdent();
		}

		try {

			int commnd = -1;
			for (@SuppressWarnings("unchecked")
			Enumeration<String> oPropNames = oMsg.getPropertyNames();
					oPropNames.hasMoreElements() && commnd<0;) {
				if (oPropNames.nextElement().equals("command"))
					commnd = oMsg.getIntProperty("command");
			} // next

			if (oMsg instanceof ObjectMessage) {

				Object oObj = ((ObjectMessage) oMsg).getObject();

				switch (commnd) {

				case COMMAND_INSERT_RECORD:
					@SuppressWarnings("unchecked")
					Pair<Record,Param[]> oPir = ((Pair<Record,Param[]>) oObj);
					try (Table tbl = dts.openTable(oPir.$1())) {
						tbl.insert(oPir.$2());
					}
					break;
					
				case COMMAND_UPDATE_RECORD:
					@SuppressWarnings("unchecked")
					Triplet<Record,Param[],Param[]> oTir = ((Triplet<Record,Param[],Param[]>) oObj);
					try (IndexableTable tbl = dts.openIndexedTable(oTir.$1())) {
						tbl.update(oTir.$2(), oTir.$3());
					}
					break;
				
				case COMMAND_STORE_RECORD:
				case COMMAND_DELETE_RECORDS:
					Record [] aRecs;
					if (oObj instanceof Record[]) {
						aRecs = (Record[]) oObj;
					} else {
						aRecs = new Record[]{(Record) oObj};
					}
					final int nRecs = aRecs.length;

					for (int r=0; r<nRecs; r++) {
						try {
							Record oRec = aRecs[r];
							if (null==itbl)
								itbl = dts.openIndexedTable(oRec);

							if (COMMAND_STORE_RECORD==commnd) {

								itbl.store(oRec);

							} else if (COMMAND_DELETE_RECORDS==commnd) {

								String[] aKeys = oMsg.getStringProperty("keys").split("`");
								if (null!=aKeys) {
									for (int k=0; k<aKeys.length; k++) {
										oRec.setKey(aKeys[k]);
										itbl.delete(oRec);
									} // next
								} // fi

							} // fi

							if (r==nRecs-1) {
								itbl.close();
								itbl = null;						
							} else if (!aRecs[r].getTableName().equals(aRecs[r+1].getTableName())) {
								itbl.close();
								itbl = null;
							}

							if (DebugFile.trace) DebugFile.writeln("record successfully "+(commnd==COMMAND_STORE_RECORD ? "stored" : "deleted"));

						} catch (Exception oXcpt) {
							if (DebugFile.trace) DebugFile.writeln(oXcpt.getClass().getName()+" "+oXcpt.getMessage());
							try { if (DebugFile.trace) DebugFile.writeln(com.knowgate.debug.StackTraceUtil.getStackTrace(oXcpt)); } catch (Exception ignore) {}
							if (itbl!=null) {
								if (DebugFile.trace) DebugFile.writeln("gracefully closing connection");
								try { itbl.close(); } catch (Exception ignore) { }
								itbl = null;
							}
						}
					} // next
					break;

				default:
					if (-1==commnd)
						throw new UnsupportedOperationException("Command property not found");
					else
						throw new UnsupportedOperationException("Command "+String.valueOf(commnd)+" not found");
				}

			} else if (oMsg instanceof TextMessage) {

				TextMessage oTxt = (TextMessage) oMsg;
				if (DebugFile.trace) DebugFile.writeln("processing text message "+oTxt.getText());

				switch (commnd) {

				case COMMAND_STOP:
					break;

				default:
					if (-1==commnd)
						throw new UnsupportedOperationException("Command property not found");
					else
						throw new UnsupportedOperationException("Command "+String.valueOf(commnd)+" not found");
				}

			} else {
				throw new ClassNotFoundException("Could not handle messages of type "+oMsg.getClass().getName());
			}

			rpl = (TemporaryQueue) oMsg.getJMSReplyTo();

			if (rpl!=null && sssn!=null) {
				if (DebugFile.trace) DebugFile.writeln("replying message "+oMsg.getJMSMessageID()+" to "+rpl.getQueueName()+(serr==null ? "" : " with error "+serr));
				rpr = sssn.createProducer(rpl);
				TextMessage oTxt = sssn.createTextMessage();
				oTxt.setBooleanProperty("Error", serr!=null);
				oTxt.setText(serr==null ? "Acknowledge" : serr);
				oTxt.setJMSCorrelationID(oMsg.getJMSMessageID());
				rpr.send(oTxt);
				rpr.close();
				rpr=null;
			} else {
				if (DebugFile.trace) DebugFile.writeln("no reply destination set for messsage "+oMsg.getJMSMessageID());
			}

		} catch (Exception xcpt) {

			if (DebugFile.trace)
				DebugFile.writeln(xcpt.getClass().getName()+" "+xcpt.getMessage());
			try { if (DebugFile.trace) DebugFile.writeln(com.knowgate.debug.StackTraceUtil.getStackTrace(xcpt)); } catch (Exception ignore) {}

			try {
				rpl = (TemporaryQueue) oMsg.getJMSReplyTo();

				if (rpl!=null && sssn!=null) {
					if (DebugFile.trace) DebugFile.writeln("replying message "+oMsg.getJMSMessageID()+" to "+rpl.getQueueName()+" with error "+xcpt.getClass().getName()+" "+xcpt.getMessage());
					rpr = sssn.createProducer(rpl);
					TextMessage oTxt = sssn.createTextMessage();
					oTxt.setBooleanProperty("Error", true);
					oTxt.setText(xcpt.getClass().getName()+" "+xcpt.getMessage());
					oTxt.setJMSCorrelationID(oMsg.getJMSMessageID());
					rpr.send(oTxt);
					rpr.close();
					rpr=null;
				}

			} catch (Exception ignore) { }

		} finally {
			if (itbl!=null) { try { itbl.close(); } catch (Exception ignore) {} }
			if (rpr!=null) { try { rpr.close(); } catch (Exception ignore) {} }
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JMSQueueListener.onMessage()");
		}
	} // onMessage

	public final static int COMMAND_STOP = 0;
	public final static int COMMAND_STORE_RECORD = 1;
	public final static int COMMAND_DELETE_RECORDS = 4;
	public final static int COMMAND_INSERT_RECORD = 8;
	public final static int COMMAND_UPDATE_RECORD = 16;

}