package org.judal.jmsqueue;

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

import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Message;
import javax.jdo.JDOException;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import javax.jms.ObjectMessage;
import javax.jms.QueueReceiver;
import javax.jms.QueueRequestor;
import javax.jms.TemporaryQueue;
import javax.jms.QueueConnection;
import javax.jms.MessageProducer;

import com.sun.messaging.ConnectionFactory;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import static com.knowgate.stringutils.Uid.createUniqueKey;

import java.io.IOException;
import java.io.Serializable;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Properties;

import org.judal.jms.ObjectMessageImpl;
import org.judal.storage.Param;
import org.judal.storage.queue.RecordQueueProducer;
import org.judal.storage.table.Record;

import com.knowgate.debug.DebugFile;
import com.knowgate.debug.StackTraceUtil;
import com.knowgate.tuples.Pair;
import com.knowgate.tuples.Triplet;

public class JMSQueueProducer implements RecordQueueProducer {

  private String sLoginId;
  private String sAuthStr;
  private Hashtable<String,String> oEnv;
  private Context oCtx;
  private ConnectionFactory oCnf;
  private Queue oQue;
  private JMSQueueListener oRql;
  private Properties oDefaultProps = new Properties();
  
  public JMSQueueProducer(String engineName, Map<String,String> properties) throws JDOException, InstantiationException {
    oEnv = new Hashtable<String,String>();
    oCtx = null;
    oCnf = null;
    oQue = null;
    oRql = new JMSQueueListener(engineName, properties, null, null);
  }
  
  public JMSQueueProducer(String engineName, String connectionFactoryName, String queueName,
  							 String directoryPath, String userId, String passwd)
  	throws NamingException,JDOException,InstantiationException {
  	sLoginId = userId;
  	sAuthStr = passwd;
    oEnv = new Hashtable<String,String>();
    if (connectionFactoryName!=null && queueName!=null && directoryPath!=null) {
      oEnv.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
      oEnv.put(Context.PROVIDER_URL, "file://"+directoryPath);
      oCtx = new InitialContext(oEnv);
      oCnf = (ConnectionFactory) oCtx.lookup(connectionFactoryName);	
      oQue = (Queue) oCtx.lookup(queueName);
      oRql = null;
    } else {
      oRql = new JMSQueueListener(engineName, new HashMap<String,String>(), null, null);
    }
  }

  public JMSQueueProducer(Properties props)
  	throws NamingException,JDOException,InstantiationException {
  	this(props.getProperty("engine"),
  		 props.getProperty("jmsconnectionfactory"),
  		 props.getProperty("jmsqueue"),
  		 props.getProperty("jmsprovider"),
  		 props.getProperty("jmsuser"),
  		 props.getProperty("jmspassword"));
  }

  protected void finalize() {
    try { close(); } catch (Exception ignore) { }
  }  

  public void close() throws JDOException {
    if (null!=oRql) oRql.close();	
  }

  private void setProperties(ObjectMessage oMsg, Properties oProps) throws JMSException {
  	
  	if (DebugFile.trace) {
  	  DebugFile.writeln("Begin RecordQueueProducer.setProperties()");
  	  DebugFile.incIdent();
  	}
  	
	oMsg.setBooleanProperty("synchronous", false);
	oMsg.setJMSMessageID(createUniqueKey());

  	if (oProps!=null) {
	  Iterator oItr = oProps.keySet().iterator();
	  while (oItr.hasNext()) {
	    String sKey = (String)oItr.next();
	  	if (sKey.equalsIgnoreCase("synchronous") || sKey.equalsIgnoreCase("sync")) {
	  	  String sVal = oProps.getProperty(sKey);
	  	  if ((sVal.equalsIgnoreCase("true") ||
	  	    sVal.equalsIgnoreCase("1") || 
	  	    sVal.equalsIgnoreCase("yes") ||
	  	    sVal.equalsIgnoreCase("synchronous"))) {
	  	    if (DebugFile.trace)
  	          DebugFile.writeln("ObjectMessage.setBooleanProperty(synchronous, true)");
	  	    oMsg.setBooleanProperty("synchronous", true);
	  	  } // fi
	  	} else {
	  	  if (DebugFile.trace)
  	        DebugFile.writeln("ObjectMessage.setStringProperty("+sKey+","+oProps.getProperty(sKey)+")");
	  	  oMsg.setStringProperty(sKey, oProps.getProperty(sKey));
	  	}
	  } // wend
	} // fi

  	if (DebugFile.trace) {
  	  DebugFile.decIdent();
  	  DebugFile.writeln("End RecordQueueProducer.setProperties()");
  	}
  } // setProperties
  
  private void sendMessage(Serializable oObj,int iCommand,Properties oProps) throws JDOException {

	QueueConnection oQcn = null;
	Session oSes = null;
	MessageProducer oMpr = null;
    QueueReceiver oQrr = null;
    QueueRequestor oQrq = null;

  	if (DebugFile.trace) {
  	  DebugFile.writeln("Begin RecordQueueProducer.sendMessage([Serializable],"+String.valueOf(iCommand)+",[Properties])");
  	  DebugFile.incIdent();
  	}
	
	if (oQue==null) {

	  ObjectMessageImpl oMsg = new ObjectMessageImpl();
	  try {
		oMsg.setObject(oObj);
		oMsg.setIntProperty("command", iCommand);
		setProperties(oMsg,oProps);
	  } catch (JMSException jmse) {
	    throw new JDOException(jmse.getMessage(), jmse);
	  }
	  oRql.onMessage(oMsg);
  	  	
	} else {

	  try {
	    oQcn = oCnf.createQueueConnection(sLoginId,sAuthStr);
	    oSes = oQcn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
	    ObjectMessage oMsg = oSes.createObjectMessage(oObj);
	    oMsg.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
	    oMsg.setIntProperty("command", iCommand);
	    setProperties(oMsg,oProps);

		if (oMsg.getBooleanProperty("synchronous")) {
	      oQcn.start();
		  TemporaryQueue oTqe = oSes.createTemporaryQueue();
	      oMsg.setJMSPriority(PRIORITY_EXPEDITED);
	  	  oMsg.setJMSReplyTo(oTqe);

	      oMpr = oSes.createProducer(oQue);
	      oMpr.send(oMsg);
		  if (DebugFile.trace)
  	  	    DebugFile.writeln("Requested message "+oMsg.getJMSMessageID()+" with reply to "+oTqe.getQueueName());

    	  oQrr = (QueueReceiver) oSes.createConsumer(oTqe); // "JMSCorrelationID='"+oMsg.getJMSMessageID()+"'"
		  TextMessage oRpl = (TextMessage) oQrr.receive(20000l);
		  oQrr.close();
		  oQrr=null;
	      oMpr.close();
	      oMpr = null;
		  oTqe.delete();      
  	  	  oTqe=null;
	      oQcn.stop();

		  if (oRpl==null) {

		    if (DebugFile.trace) {
  	  	      DebugFile.writeln("Reply timed out");
		      DebugFile.decIdent();
		    }
  	  	    throw new JDOException("Message reply timed out");

		  } else if (oRpl.getBooleanProperty("Error")) {

		    if (DebugFile.trace) {
  	  	      DebugFile.writeln(oRpl.getText());
		      DebugFile.decIdent();
		    }
			throw new JDOException(oRpl.getText());

		  }

		} else {

	      oMsg.setJMSPriority(PRIORITY_NORMAL);
	      oMpr = oSes.createProducer(oQue);
	      oMpr.send(oMsg);
		  if (DebugFile.trace)
  	  	    DebugFile.writeln("Sent message "+oMsg.getJMSMessageID());
	      oMpr.close();
	      oMpr = null;

		}

	    oSes.close();
	    oSes = null;
	    oQcn.close();
	    oQcn = null;
	  } catch (JMSException jmse) {
		throw new JDOException(jmse.getMessage(), jmse);
	  } finally {
	    if (null!=oQrq) { try {oQrq.close(); } catch (Exception ignore) {} }
	    if (null!=oQrr) { try {oQrr.close(); } catch (Exception ignore) {} }
	    if (null!=oMpr) { try {oMpr.close(); } catch (Exception ignore) {} }
	    if (null!=oSes) { try {oSes.close(); } catch (Exception ignore) {} }
	    if (null!=oQcn) { try {oQcn.close(); } catch (Exception ignore) {} }
	  }
	} // fi

  	if (DebugFile.trace) {
  	  DebugFile.decIdent();
  	  DebugFile.writeln("End RecordQueueProducer.sendMessage()");
  	}

  } // store

  public void store(Record oRec) throws JDOException {
  	if (DebugFile.trace) {
  	  DebugFile.writeln("RecordQueueProducer.store("+oRec.getTableName()+"."+oRec.getKey()+")");
  	}
    if (!oDefaultProps.containsKey("useraccount")) oDefaultProps.put("useraccount","anonymous");    
    sendMessage(oRec,COMMAND_STORE_RECORD,null);
  }

  public void store(Record[] aRecs) throws JDOException {
    if (!oDefaultProps.containsKey("useraccount")) oDefaultProps.put("useraccount","anonymous");    
	  sendMessage(aRecs,COMMAND_STORE_RECORD,null);
  }

  public void store(Record oRec,Properties oProps) throws JDOException {
  	if (DebugFile.trace) {
  	  DebugFile.writeln("RecordQueueProducer.store("+oRec.getTableName()+"."+oRec.getKey()+","+oProps+")");
  	}
    if (oProps==null) oProps = oDefaultProps;
    if (!oProps.containsKey("useraccount")) oProps.put("useraccount","anonymous");
    sendMessage(oRec,COMMAND_STORE_RECORD,oProps);
  }

  public void store(Record[] aRecs, Properties oProps) throws JDOException {
    if (oProps==null) oProps = oDefaultProps;
	if (!oProps.containsKey("useraccount")) oProps.put("useraccount","anonymous");
	sendMessage(aRecs,COMMAND_STORE_RECORD,oProps);
  }

  public void insert(Record oRec, Param[] aParams) throws JDOException {
    if (!oDefaultProps.containsKey("useraccount")) oDefaultProps.put("useraccount","anonymous");    
	  sendMessage(new Pair<Record,Param[]>(oRec, aParams),COMMAND_INSERT_RECORD, null);  	
  }
  
  public void insert(Record oRec, Param[] aParams, Properties oProps) throws JDOException {
    if (!oDefaultProps.containsKey("useraccount")) oDefaultProps.put("useraccount","anonymous");    
	  sendMessage(new Pair<Record,Param[]>(oRec, aParams),COMMAND_INSERT_RECORD,oProps);  	  	
  }
  
  public void update(Record oRec, Param[] aParams, Param[] aWhere) throws JDOException {
    if (!oDefaultProps.containsKey("useraccount")) oDefaultProps.put("useraccount","anonymous");    
	  sendMessage(new Triplet<Record,Param[],Param[]>(oRec, aParams, aWhere),COMMAND_UPDATE_RECORD, null);
  }
  
  public void update(Record oRec, Param[] aParams, Param[] aWhere, Properties oProps) throws JDOException {
    if (!oDefaultProps.containsKey("useraccount")) oDefaultProps.put("useraccount","anonymous");    
	  sendMessage(new Triplet<Record,Param[],Param[]>(oRec, aParams, aWhere),COMMAND_UPDATE_RECORD, oProps);  	
  }
  
  public void delete(Record oRec, String[] aKeys, Properties oProps) throws JDOException {
    if (oProps==null) oProps = oDefaultProps;
    if (!oProps.containsKey("useraccount")) oProps.put("useraccount","anonymous");
    StringBuffer buff = new StringBuffer();
    boolean first = true;
    for (String k : aKeys) {
    	if (first)
    		first = false;
    	else
        	buff.append("`");
    	buff.append(k);
    }
    oProps.put("keys",buff.toString());
    sendMessage(oRec,COMMAND_DELETE_RECORDS,oProps);
  }

  public void stop(boolean bInmediate, int iTimeout) throws JDOException {
	Connection oQcn = null;
	Session oSes = null;
	MessageProducer oMpr = null;
    QueueReceiver oQrr = null;
    TemporaryQueue oRpl = null;

  	if (DebugFile.trace) {
  	  DebugFile.writeln("Begin RecordQueueProducer.stop("+String.valueOf(bInmediate)+","+String.valueOf(iTimeout)+")");
  	  DebugFile.incIdent();
  	}
	
	if (oQue!=null) {
	  try {
	    oQcn = oCnf.createConnection(sLoginId,sAuthStr);
	    oSes = (Session) oQcn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	    TextMessage oMsg = oSes.createTextMessage("Stop");
	    oMsg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
	    oMsg.setIntProperty("command", COMMAND_STOP);
	    oMsg.setJMSPriority(bInmediate ? 9 : 0);
	    if (iTimeout>0) {
		  oRpl = oSes.createTemporaryQueue();
	  	  oMsg.setJMSReplyTo(oRpl);
	    }
	    oMpr = oSes.createProducer(oQue);
	    oMpr.send(oMsg);
	    oMpr.close();
	    oMpr = null;

		if (DebugFile.trace)
  	  	    DebugFile.writeln("Sent stop message "+oMsg.getJMSMessageID());

		if (iTimeout>0) {
		  oQcn.start();
    	  oQrr = (QueueReceiver) oSes.createConsumer(oRpl, "JMSCorrelationID='"+oMsg.getJMSMessageID()+"'");
		  Message oMss = oQrr.receive(iTimeout);
		  if (null==oMss) throw new JMSException("Stop request timed out");
		  if (DebugFile.trace)
  	  	    DebugFile.writeln("Stop completed "+oMsg.getJMSMessageID()+" "+oMss.getJMSCorrelationID());
		  oQrr.close();
		  oQrr=null;
		  oRpl.delete();
		  oRpl=null;
		  oQcn.stop();
		}

	    oSes.close();
	    oSes = null;
	    oQcn.close();
	    oQcn = null;
	  } catch (JMSException jmse) {
        if (DebugFile.trace) {
            DebugFile.writeln("JMSQueueProducer.stop() JMSException " + jmse.getMessage());
            try {
				DebugFile.writeln(StackTraceUtil.getStackTrace(jmse));
			} catch (IOException ignore) { }
            DebugFile.decIdent();
          }
    	throw new JDOException(jmse.getMessage(), jmse);
	  } finally {
	    if (null!=oQrr) { try {oQrr.close(); } catch (Exception ignore) {} }
	    if (null!=oMpr) { try {oMpr.close(); } catch (Exception ignore) {} }
	    if (null!=oRpl) { try {oRpl.delete();} catch (Exception ignore) {} }
	    if (null!=oSes) { try {oSes.close(); } catch (Exception ignore) {} }
	    if (null!=oQcn) { try {oQcn.close(); } catch (Exception ignore) {} }
	  }
	} // fi

  	if (DebugFile.trace) {
  	  DebugFile.decIdent();
  	  DebugFile.writeln("End RecordQueueProducer.stop()");
  	}
  } // stop

  public final static int COMMAND_STOP = 0;
  public final static int COMMAND_STORE_RECORD = 1;
  public final static int COMMAND_DELETE_RECORDS = 4;
  public final static int COMMAND_INSERT_RECORD = 8;
  public final static int COMMAND_UPDATE_RECORD = 16;

  public final static int PRIORITY_LOW = 1;
  public final static int PRIORITY_NORMAL = 4;
  public final static int PRIORITY_EXPEDITED = 7;
  
}
