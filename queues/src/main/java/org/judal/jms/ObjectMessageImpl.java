package org.judal.jms;

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

import javax.jms.ObjectMessage;
import javax.transaction.TransactionManager;

import org.judal.storage.Param;
import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.Record;
import org.judal.storage.table.Table;
import org.judal.storage.table.TableDataSource;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.jms.JMSException;
import javax.jms.Destination;

import com.knowgate.debug.DebugFile;
import com.knowgate.debug.StackTraceUtil;
import com.knowgate.tuples.Pair;
import com.knowgate.tuples.Triplet;

import java.util.Date;
import java.util.Hashtable;
import java.util.Enumeration;

/**
 * ObjectMessage implementation
 * @author Sergio Montoro Ten
 *
 */
public class ObjectMessageImpl implements ObjectMessage, Runnable {

	private Serializable oObj;
	private String sId;
	private String sCid;
	private String sJtp;
	private int iDlm;
	private int iPrt;
	private boolean bRdl;
	private long lTs;
	private long lExp;
	private Destination replyTo;
	private Destination jmsDestination;
	private TableDataSource oDts;
	private Hashtable<String, Object> oPrp;

	public ObjectMessageImpl() {
		oDts = null;
		oPrp = new Hashtable<String, Object>();
		replyTo = null;
	}

	public ObjectMessageImpl(TableDataSource oDtsr) {
		oDts = oDtsr;
		oPrp = new Hashtable<String, Object>();
		replyTo = null;
	}

	/**
	 * @param parm1 Serializable Instance of Record, Record[], com.knowgate.tuples.Pair&lt;Record,Param[]&gt; or com.knowgate.tuples.Triplet&lt;Record,Param[],Param[]&gt;
	 * @throws JMSException
	 */
	public void setObject(Serializable parm1) throws JMSException {
		oObj = parm1;
	}

	/**
	 * @throws JMSException
	 * @return Serializable Instance of Record, Record[], com.knowgate.tuples.Pair&lt;Record,Param[]&gt; or com.knowgate.tuples.Triplet&lt;Record,Param[],Param[]&gt;
	 */
	public Serializable getObject() throws JMSException {
		return oObj;
	}

	/**
	 * @throws JMSException
	 * @return String
	 */
	public String getJMSMessageID() throws JMSException {
		return sId;
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 */
	public void setJMSMessageID(String parm1) throws JMSException {
		sId = parm1;
	}

	/**
	 * @throws JMSException
	 * @return long
	 */
	public long getJMSTimestamp() throws JMSException {
		return lTs;
	}

	/**
	 * @param parm1 long
	 * @throws JMSException
	 */
	public void setJMSTimestamp(long parm1) throws JMSException {
		lTs = parm1;
	}

	/**
	 * @throws JMSException
	 * @return byte[]
	 */
	public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
		return sCid.getBytes();
	}

	/**
	 * @param parm1 byte[]
	 * @throws JMSException
	 */
	public void setJMSCorrelationIDAsBytes(byte[] parm1) throws JMSException {
		sCid = new String(parm1);
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 */
	public void setJMSCorrelationID(String parm1) throws JMSException {
		sCid = parm1;
	}

	/**
	 * @throws JMSException
	 * @return String
	 */
	public String getJMSCorrelationID() throws JMSException {
		return sCid;
	}

	/**
	 * This method always returns <b>null</b>.
	 * @throws JMSException
	 * @return Destination <b>null</b>
	 */
	public Destination getJMSReplyTo() throws JMSException {
		return replyTo;
	}

	/**
	 * @param parm1 Destination
	 * @throws JMSException
	 */
	public void setJMSReplyTo(Destination parm1) throws JMSException {
		replyTo = parm1;
	}

	/**
	 * @throws JMSException
	 * @return Destination
	 */
	public Destination getJMSDestination() throws JMSException {
		return jmsDestination;
	}

	/**
	 * @param parm1 Destination
	 * @throws JMSException
	 */
	public void setJMSDestination(Destination parm1) throws JMSException {
		jmsDestination = parm1;
	}

	/**
	 * @throws JMSException
	 * @return int
	 */
	public int getJMSDeliveryMode() throws JMSException {
		return iDlm;
	}

	/**
	 * @param parm1 int
	 * @throws JMSException
	 */
	public void setJMSDeliveryMode(int parm1) throws JMSException {
		iDlm = parm1;
	}

	/**
	 * @throws JMSException
	 * @return boolean
	 */
	public boolean getJMSRedelivered() throws JMSException {
		return bRdl;
	}

	/**
	 * @param parm1 boolean
	 * @throws JMSException
	 */
	public void setJMSRedelivered(boolean parm1) throws JMSException {
		bRdl = parm1;
	}

	/**
	 * @throws JMSException
	 * @return String
	 */
	public String getJMSType() throws JMSException {
		return sJtp;
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 */
	public void setJMSType(String parm1) throws JMSException {
		sJtp = parm1;
	}

	/**
	 * @throws JMSException
	 * @return long
	 */
	public long getJMSExpiration() throws JMSException {
		return lExp;
	}

	/**
	 * @param parm1 long
	 * @throws JMSException
	 */
	public void setJMSExpiration(long parm1) throws JMSException {
		lExp = parm1;
	}

	/**
	 * @throws JMSException
	 * @return int
	 */
	public int getJMSPriority() throws JMSException {
		return iPrt;
	}

	/**
	 * @param parm1 int
	 * @throws JMSException
	 */
	public void setJMSPriority(int parm1) throws JMSException {
		iPrt = parm1;
	}

	/**
	 * @throws JMSException
	 */
	public void clearProperties() throws JMSException {
		oPrp.clear();
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 * @return boolean
	 *
	 */
	public boolean propertyExists(String parm1) throws JMSException {
		return oPrp.containsKey(parm1);
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 * @return boolean
	 */
	public boolean getBooleanProperty(String parm1) throws JMSException {
		return ((Boolean) oPrp.get(parm1)).booleanValue();
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 * @return byte
	 */
	public byte getByteProperty(String parm1) throws JMSException {
		return ((Byte) oPrp.get(parm1)).byteValue();
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 * @return short
	 */
	public short getShortProperty(String parm1) throws JMSException {
		return ((Short) oPrp.get(parm1)).shortValue();
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 * @return int
	 */
	public int getIntProperty(String parm1) throws JMSException {
		return ((Integer) oPrp.get(parm1)).intValue();
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 * @return long
	 */
	public long getLongProperty(String parm1) throws JMSException {
		return ((Long) oPrp.get(parm1)).longValue();
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 * @return float
	 */
	public float getFloatProperty(String parm1) throws JMSException {
		return ((Float) oPrp.get(parm1)).floatValue();
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 * @return double
	 */
	public double getDoubleProperty(String parm1) throws JMSException {
		return ((Double) oPrp.get(parm1)).doubleValue();
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 * @return String
	 */
	public String getStringProperty(String parm1) throws JMSException {
		return (String) oPrp.get(parm1);
	}

	/**
	 * @param parm1 String
	 * @throws JMSException
	 * @return Object
	 */
	public Object getObjectProperty(String parm1) throws JMSException {
		return oPrp.get(parm1);
	}

	/**
	 * @throws JMSException
	 * @return Enumeration&lt;String&gt;
	 */
	public Enumeration<String> getPropertyNames() throws JMSException {
		return oPrp.keys();
	}

	/**
	 * @param parm1 String
	 * @param parm2 boolean
	 * @throws JMSException
	 */
	public void setBooleanProperty(String parm1, boolean parm2) throws JMSException {
		oPrp.put(parm1, new Boolean(parm2));
	}

	/**
	 * @param parm1 String
	 * @param parm2 byte
	 * @throws JMSException
	 */
	public void setByteProperty(String parm1, byte parm2) throws JMSException {
		oPrp.put(parm1, new Byte(parm2));
	}

	/**
	 * @param parm1 String
	 * @param parm2 short
	 * @throws JMSException
	 */
	public void setShortProperty(String parm1, short parm2) throws JMSException {
		oPrp.put(parm1, new Short(parm2));
	}

	/**
	 * <p>Set int property.</p>
	 * The "command" property will determine the behavior of the run method with one of
	 * { COMMAND_STORE_RECORD, COMMAND_DELETE_RECORDS, COMMAND_INSERT_RECORD, COMMAND_UPDATE_RECORD }
	 * @param parm1 String
	 * @param parm2 int
	 * @throws JMSException
	 */
	public void setIntProperty(String parm1, int parm2) throws JMSException {
		oPrp.put(parm1, new Integer(parm2));
	}

	/**
	 * @param parm1 String
	 * @param parm2 long
	 * @throws JMSException
	 */
	public void setLongProperty(String parm1, long parm2) throws JMSException {
		oPrp.put(parm1, new Long(parm2));
	}

	/**
	 * @param parm1 String
	 * @param parm2 float
	 * @throws JMSException
	 */
	public void setFloatProperty(String parm1, float parm2) throws JMSException {
		oPrp.put(parm1, new Float(parm2));
	}

	/**
	 * @param parm1 String
	 * @param parm2 double
	 * @throws JMSException
	 */
	public void setDoubleProperty(String parm1, double parm2) throws JMSException {
		oPrp.put(parm1, new Double(parm2));
	}

	/**
	 * @param parm1 String
	 * @param parm2 String
	 * @throws JMSException
	 */
	public void setStringProperty(String parm1, String parm2) throws JMSException {
		oPrp.put(parm1, parm2);
	}

	/**
	 * @param parm1 String
	 * @param parm2  Object
	 * @throws JMSException
	 */
	public void setObjectProperty(String parm1, Object parm2) throws JMSException {
		oPrp.put(parm1, parm2);
	}

	/**
	 * @throws JMSException
	 */
	public void acknowledge() throws JMSException {
		// TODO: Add your code here
	}

	/**
	 * @throws JMSException
	 */
	public void clearBody() throws JMSException {
		oObj = null;
	}

	/**
	 * <p>Run operation on this object.</p>
	 * This method will perform the following actions:<br/>
	 * <ol>
	 * <li>Cast getObject() into the appropriate number of Record and Param[] instances</li>
	 * <li>Call getIntProperty("command")</li>
	 * <li>If command is COMMAND_STORE_RECORD Then call Record.store(TableDataSource).</li>
	 * <li>Else If command is COMMAND_DELETE_RECORD Then call Record.store(TableDataSource).</li>
	 * <li>Else If command is COMMAND_INSERT_RECORD Then call Table.insert(Param[]).</li>
	 * <li>Else If command is COMMAND_UPDATE_RECORD Then call IndexableTable.update(Param[],Param[]).</li>
	 * </ol>
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void run() {
		TransactionManager oTrnMan = oDts.getTransactionManager();

		try {
			final int iCmd = getIntProperty("command");
			Object oObj = getObject();
			int nRecs = 0;
			int nParams = 0;
			Record oRec = null;
			Record[] aRecs = null;
			Param[] aParms = null;
			Param[] aWhere = null;

			if (oObj instanceof Record[]) {
				aRecs = (Record[]) oObj;
				nRecs = aRecs.length;
			} else if (oObj instanceof Record) {
				aRecs = new Record[] { (Record) oObj };
				nRecs = aRecs.length;
			} else if (oObj instanceof Pair<?, ?>) {
				oRec = ((Pair<Record,Param[]>) oObj).$1();
				aParms = ((Pair<Record,Param[]>) oObj).$2();
				nParams = aParms.length;
			} else if (oObj instanceof Triplet<?, ?, ?>) {
				oRec = ((Triplet<Record,Param[],Param[]>) oObj).$1();
				aParms = ((Triplet<Record,Param[],Param[]>) oObj).$2();
				nParams = aParms.length;
				aWhere = ((Triplet<Record,Param[],Param[]>) oObj).$3();
			}

			if (DebugFile.trace) {
				DebugFile.writeln("Begin ObjectMessageImpl.run(" + getJMSMessageID() + "," + this + ","
						+ new Date(this.getJMSTimestamp()).toString() + ")");
				String sCmd, sRtp;
				switch (iCmd) {
				case COMMAND_STORE_RECORD:
					sCmd = "STORE RECORD COMMAND";
					break;
				case COMMAND_DELETE_RECORDS:
					sCmd = "DELETE RECORDS COMMAND";
					break;
				case COMMAND_INSERT_RECORD:
					sCmd = "INSERT RECORD COMMAND";
					break;
				case COMMAND_UPDATE_RECORD:
					sCmd = "UPDATE RECORD COMMAND";
					break;
				default:
					sCmd = "UNKNOWN COMMAND";
					break;
				}
				if (nRecs > 0)
					sRtp = aRecs[0].getClass().getName();
				else if (oRec != null)
					sRtp = oRec.getClass().getName();
				else
					sRtp = "null";
				DebugFile.writeln("Perform " + sCmd + " on " + String.valueOf(nRecs) + " record"
						+ (nRecs > 1 ? "s" : "") + " of class " + sRtp);
			}
			
			if (oTrnMan!=null)
				oTrnMan.begin();

			for (int r = 0; r < nRecs; r++) {
				oRec = aRecs[r];

				if (oRec == null) {

					switch (iCmd) {
					case COMMAND_STORE_RECORD:
						if (DebugFile.trace)
							DebugFile.writeln("COMMAND_STORE_RECORD NullPointerException Record is null");
						break;
					case COMMAND_DELETE_RECORDS:
						if (DebugFile.trace)
							DebugFile.writeln("COMMAND_DELETE_RECORDS NullPointerException Record is null");
						break;
					} // end switch

				} else {

					if (DebugFile.trace)
						DebugFile.writeln("executing command " + String.valueOf(iCmd) + " for record " + oRec.getKey());

					if (DebugFile.trace)
						DebugFile.writeln("table " + oRec.getTableName() + " opened");

					switch (iCmd) {
					case COMMAND_STORE_RECORD:
						if (DebugFile.trace)
							DebugFile.writeln("COMMAND_STORE_RECORD " + oRec.getTableName() + "(" + oRec.getKey() + ")");
						oRec.store(oDts);
						break;

					case COMMAND_DELETE_RECORDS:
						if (DebugFile.trace)
							DebugFile.writeln("COMMAND_DELETE_RECORDS { " + getStringProperty("keys") + "} from " + oRec.getTableName());
						String[] aKeys = getStringProperty("keys").split("`");
						for (int k = aKeys.length-1; k >= 0; k--) {
							if (aKeys[k]!=null & aKeys[k].trim().length()>0) {
								if (DebugFile.trace) {
									DebugFile.writeln("Deleting record " + aKeys[k] + " on "+ oRec.getTableName());
								}
								oRec.setKey(aKeys[k]);
								oRec.delete(oDts);
								if (DebugFile.trace) {
									try (Table oTbd = oDts.openTable(oRec)) {
										if (oTbd.exists(aKeys[k])) {
											DebugFile.writeln("Error deleting " + oRec.getClass().getName() + " with key " + aKeys[k] + " on table " + oRec.getTableName());
										}
									}
								}								
							}
						}
						break;
					} // end switch

				}
			} // next

			if (nParams > 0) {
				switch (iCmd) {
				case COMMAND_UPDATE_RECORD:
					if (DebugFile.trace)
						DebugFile.writeln("COMMAND_UPDATE_RECORD FROM " + oRec.getTableName());
					if (DebugFile.trace)
						DebugFile.writeln("opening table " + oRec.getTableName() + " from DataSource " + oDts + " for update");
					try (IndexableTable oITbl = oDts.openIndexedTable(oRec)) {
						oITbl.update(aParms, aWhere);
					}
					break;

				case COMMAND_INSERT_RECORD:
					if (DebugFile.trace)
						DebugFile.writeln("COMMAND_INSERT_RECORD INTO " + oRec.getTableName());
					if (DebugFile.trace)
						DebugFile.writeln("opening table " + oRec.getTableName() + " from DataSource " + oDts + " for insert");
					try (Table oTbl = oDts.openTable(oRec)) {
						oTbl.insert(aParms);						
					}
					break;
				} // end switch
			}

			if (oTrnMan!=null)
				oTrnMan.commit();
			
		} catch (Exception xcpt) {
			if (DebugFile.trace) {
				DebugFile.writeln("ObjectMessageImpl.run() " + xcpt.getClass().getName() + " " + xcpt.getMessage());
				DebugFile.writeStackTrace(xcpt);
			}
			try {
				if (oTrnMan!=null)
					if (oDts.inTransaction()) oTrnMan.rollback();
			} catch (Exception rbx) {
				if (DebugFile.trace)
					DebugFile.writeln(rbx.getClass().getName()+" at TransactionManager.rollback() "+rbx.getMessage());
			}			
		} finally {
			if (oTrnMan!=null) {
				try {
					Method reset = oTrnMan.getClass().getMethod("reset");
					if (reset!=null)
						reset.invoke(oTrnMan);
				} catch (NoSuchMethodException | SecurityException | IllegalAccessException |
						 IllegalArgumentException | InvocationTargetException ignore) { }
			}			
		}
		if (DebugFile.trace)
			DebugFile.writeln("End ObjectMessageImpl.run()");
	}

	public final static int COMMAND_STOP = 0;
	public final static int COMMAND_STORE_RECORD = 1;
	public final static int COMMAND_DELETE_RECORDS = 4;
	public final static int COMMAND_INSERT_RECORD = 8;
	public final static int COMMAND_UPDATE_RECORD = 16;

}
