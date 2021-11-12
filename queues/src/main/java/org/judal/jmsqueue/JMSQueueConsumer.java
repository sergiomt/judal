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

import java.io.IOException;
import java.util.ArrayList;

import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Message;
import javax.jdo.JDOException;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.QueueReceiver;
import javax.jms.QueueBrowser;
import com.sun.messaging.ConnectionFactory;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import java.util.Hashtable;
import java.util.Map;
import java.util.Enumeration;
import java.util.NoSuchElementException;

import org.judal.storage.DataSource;
import org.judal.storage.Engine;
import org.judal.storage.queue.RecordQueueConsumer;

import com.knowgate.debug.DebugFile;
import com.knowgate.debug.StackTraceUtil;

/**
 * RecordQueueConsumer implementation
 * @author Sergio Montoro Ten
 *
 */
public class JMSQueueConsumer implements RecordQueueConsumer {

  private boolean bSnc;
  private String sLoginId;
  private String sAuthStr;
  private Hashtable<String,String> oEnv;
  private Context oCtx;
  private String oQnm;
  private ConnectionFactory oCnf;
  private Connection oQcn;
  private Queue oQue;
  private Session oSes;
  private QueueReceiver oQrc;

  /**
   * <p>Constructor.</p>
   * Set the initial context factory to com.sun.jndi.fscontext.RefFSContextFactory
   * and the provider URL pointing to sDirectory
   * The call javax.naming.Context.lookup() on sConnectionFactoryName
   * @param sConnectionFactoryName String
   * @param sQueueName String Queue name
   * @param sDirectory String Disk path to provider URL
   * @param sUserId String
   * @param sPassword String
   * @param bSynchronous boolean
   * @throws JDOException
   * @throws NamingException
   * @throws JMSException
   */
  public JMSQueueConsumer(String sConnectionFactoryName, String sQueueName, String sDirectory,
  						  String sUserId, String sPassword, boolean bSynchronous)
  	throws JDOException,NamingException,JMSException {  		
  	sLoginId = sUserId;
  	sAuthStr = sPassword;
    oEnv = new Hashtable<String,String>();
    oEnv.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
    oEnv.put(Context.PROVIDER_URL, "file://"+sDirectory);
    oCtx = new InitialContext(oEnv);
    oCnf = (ConnectionFactory) oCtx.lookup(sConnectionFactoryName);
    oQnm = sQueueName;
    bSnc = bSynchronous;
    oQcn = null;
    oSes = null;
    oQue = null;
    oQrc = null;
  }

  protected  void finalize() {
  	try { stop(); } catch (Exception ignore) {}
  }

  /**
   * <p>Connect to queue and enumerate messages.</p>
   * @return ArrayList&lt;Message&gt;
   * @throws JMSException
   */
  public ArrayList<Message> browse() throws JMSException {

    if (DebugFile.trace) {
      DebugFile.writeln("Begin RecordQueueConsumer.browse()");
      DebugFile.incIdent();
    }
	
	Connection oCnn = oCnf.createConnection(sLoginId,sAuthStr);
	Session oSss = oCnn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Queue oQuu = oSss.createQueue(oQnm);
    QueueReceiver oQrr = (QueueReceiver) oSss.createConsumer(oQuu);
    ArrayList<Message> oMsgs = new ArrayList<Message>();
    QueueBrowser oQbr = oSss.createBrowser(oQuu);
	Enumeration oEnu = oQbr.getEnumeration();
	while (oEnu.hasMoreElements()) {
	  oMsgs.add((Message) oEnu.nextElement());
    } // wend
    oQrr.close();
    oSss.close();
    oCnn.close();

    if (DebugFile.trace) {
      DebugFile.decIdent();
      DebugFile.writeln("End RecordQueueConsumer.browse() : "+String.valueOf(oMsgs.size())+" queued messages");
    }

	return oMsgs;
  } // browse
    
  /**
   * <p>Start RecordQueueConsumer.</p>
   * Create JMS connection, session, queue and receiver.
   * If this RecordQueueConsumer was configured as synchronous when it was constructed
   * then enter a loop to immediately process the pending messages using the current Thread
   * until a STOP command is received.
   * @param engine Engine&lt;? extends DataSource&gt;
   * @param properties Map&lt;String,String&gt;
   * @throws IllegalStateException If this RecordQueueConsumer is already connected
   * @throws JDOException
   */
  @Override
  public void start(Engine<? extends DataSource> engine, Map<String,String> properties) throws JDOException, IllegalStateException {

  	Message oMsg;
	JMSQueueListener oRql;
	
	if (oQcn!=null) throw new IllegalStateException("RecordQueueConsumer is already connected");

    if (DebugFile.trace) {
      DebugFile.writeln("Begin RecordQueueConsumer.start()");
      DebugFile.incIdent();
      DebugFile.writeln("ConnectionFactory.createConnection("+sLoginId+", ...)");
    }
	
    try {
    	oQcn = oCnf.createConnection(sLoginId,sAuthStr);

        if (DebugFile.trace) DebugFile.writeln("Connection.createSession(false, Session.AUTO_ACKNOWLEDGE)");

    	oSes = oQcn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        oQue = oSes.createQueue(oQnm);

        if (DebugFile.trace) {
    	  int nMsgs=0;	  
    	  QueueBrowser oQbr = oSes.createBrowser(oQue);
    	  try {
    	    Enumeration oEnu = oQbr.getEnumeration();	  
    	    while (oEnu.hasMoreElements()) {
    	      nMsgs++;
    	      oEnu.nextElement();
            } // wend
    	  } catch (NoSuchElementException nosuch) {
            DebugFile.writeln("NoSuchElementException "+nosuch.getMessage());
    	  } finally {
            if (null!=oQbr) oQbr.close();
    	  }
          DebugFile.writeln("queue had "+String.valueOf(nMsgs)+" previous messages");
        } // fi

        oQrc = (QueueReceiver) oSes.createConsumer(oQue);
        oRql = new JMSQueueListener(engine.name(), properties, null, oSes);
        if (bSnc) {
          if (DebugFile.trace) DebugFile.writeln("Connection.start()");
          oQcn.start();
          while ((oMsg=oQrc.receive())!=null) {
            if (DebugFile.trace) DebugFile.writeln("new message "+oMsg.getJMSMessageID()+" received at "+oQrc.getQueue().getQueueName());
            int iCmd = -1;
            try {
              iCmd = oMsg.getIntProperty("command");
            } catch (NullPointerException npe) {
              if (DebugFile.trace) DebugFile.writeln("no command set at message");        
            }
            if (DebugFile.trace) DebugFile.writeln("before RecordQueueListener.onMessage("+oMsg.getJMSMessageID()+")");
          	oRql.onMessage(oMsg);
            if (iCmd == JMSQueueListener.COMMAND_STOP) {
              stop();
              break;
            }
          } // wend
        } else {
          if (DebugFile.trace) DebugFile.writeln("QueueReceiver.setMessageListener("+oRql.toString()+")");
          oQrc.setMessageListener(oRql);
          if (DebugFile.trace) DebugFile.writeln("Connection.start()");
          oQcn.start();
        }    	
    } catch (JMSException jmse) {
        if (DebugFile.trace) {
            DebugFile.writeln("JMSQueueConsumer.start() JMSException " + jmse.getMessage());
            try {
				DebugFile.writeln(StackTraceUtil.getStackTrace(jmse));
			} catch (IOException ignore) { }
            DebugFile.decIdent();
          }
    	throw new JDOException(jmse.getMessage(), jmse);
    }

    if (DebugFile.trace) {
      DebugFile.decIdent();
      DebugFile.writeln("End RecordQueueConsumer.start()");
    }
  } // start
  
  /**
   * <p>Close this RecordQueueConsumer.</p>
   * Stop the current JMS connection, close the QueueReceiver, close the Session and finally close the JMS connection.
   * @throws JDOException
   */
  @Override
  public void stop() throws JDOException {

    if (DebugFile.trace) {
      DebugFile.writeln("Begin RecordQueueConsumer.stop()");
      DebugFile.incIdent();
    }

    try {
	    if (oQcn!=null) {
		  oQcn.stop();
		}
	    if (null!=oQrc) {
	      oQrc.close();
	      oQrc = null;
	    }
	    oQue = null;
	    if (null!=oSes) {
	      oSes.close();
	      oSes = null;
	    }
		if (oQcn!=null) {
		  oQcn.close();
		  oQcn=null;
		}    	
    } catch (JMSException jmse) {
        if (DebugFile.trace) {
            DebugFile.writeln("JMSQueueConsumer.stop() JMSException " + jmse.getMessage());
            try {
				DebugFile.writeln(StackTraceUtil.getStackTrace(jmse));
			} catch (IOException ignore) { }
            DebugFile.decIdent();
          }
    	throw new JDOException(jmse.getMessage(), jmse);
    }

    if (DebugFile.trace) {
      DebugFile.decIdent();
      DebugFile.writeln("End RecordQueueConsumer.stop()");
    }
  } // stop

}
