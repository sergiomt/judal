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

import org.judal.storage.Param;
import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.Record;
import org.judal.storage.table.Table;
import org.judal.storage.table.TableDataSource;

import java.io.IOException;
import java.io.Serializable;

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
	private TableDataSource oDts;
	private Hashtable<String, Object> oPrp;

	public ObjectMessageImpl() {
		oDts = null;
		oPrp = new Hashtable<String, Object>();
	}

	public ObjectMessageImpl(TableDataSource oDtsr) {
		oDts = oDtsr;
		oPrp = new Hashtable<String, Object>();
	}

	/**
	 * Method setObject
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 */
	public void setObject(Serializable parm1) throws JMSException {
		oObj = parm1;
	}

	/**
	 * Method getObject
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public Serializable getObject() throws JMSException {
		return oObj;
	}

	/**
	 * Method getJMSMessageID
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public String getJMSMessageID() throws JMSException {
		return sId;
	}

	/**
	 * Method setJMSMessageID
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 */
	public void setJMSMessageID(String parm1) throws JMSException {
		sId = parm1;
	}

	/**
	 * Method getJMSTimestamp
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public long getJMSTimestamp() throws JMSException {
		return lTs;
	}

	/**
	 * Method setJMSTimestamp
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 */
	public void setJMSTimestamp(long parm1) throws JMSException {
		lTs = parm1;
	}

	/**
	 * Method getJMSCorrelationIDAsBytes
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
		return sCid.getBytes();
	}

	/**
	 * Method setJMSCorrelationIDAsBytes
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 */
	public void setJMSCorrelationIDAsBytes(byte[] parm1) throws JMSException {
		sCid = new String(parm1);
	}

	/**
	 * Method setJMSCorrelationID
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 */
	public void setJMSCorrelationID(String parm1) throws JMSException {
		sCid = parm1;
	}

	/**
	 * Method getJMSCorrelationID
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public String getJMSCorrelationID() throws JMSException {
		return sCid;
	}

	/**
	 * Method getJMSReplyTo
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public Destination getJMSReplyTo() throws JMSException {
		return null;
	}

	/**
	 * Method setJMSReplyTo
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 */
	public void setJMSReplyTo(Destination parm1) throws JMSException {
		// TODO: Add your code here
	}

	/**
	 * Method getJMSDestination
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public Destination getJMSDestination() throws JMSException {
		return null;
	}

	/**
	 * Method setJMSDestination
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 */
	public void setJMSDestination(Destination parm1) throws JMSException {
		// TODO: Add your code here
	}

	/**
	 * Method getJMSDeliveryMode
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public int getJMSDeliveryMode() throws JMSException {
		return iDlm;
	}

	/**
	 * Method setJMSDeliveryMode
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 */
	public void setJMSDeliveryMode(int parm1) throws JMSException {
		iDlm = parm1;
	}

	/**
	 * Method getJMSRedelivered
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public boolean getJMSRedelivered() throws JMSException {
		return bRdl;
	}

	/**
	 * Method setJMSRedelivered
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 */
	public void setJMSRedelivered(boolean parm1) throws JMSException {
		bRdl = parm1;
	}

	/**
	 * Method getJMSType
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public String getJMSType() throws JMSException {
		return sJtp;
	}

	/**
	 * Method setJMSType
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 */
	public void setJMSType(String parm1) throws JMSException {
		sJtp = parm1;
	}

	/**
	 * Method getJMSExpiration
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public long getJMSExpiration() throws JMSException {
		return lExp;
	}

	/**
	 * Method setJMSExpiration
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 */
	public void setJMSExpiration(long parm1) throws JMSException {
		lExp = parm1;
	}

	/**
	 * Method getJMSPriority
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public int getJMSPriority() throws JMSException {
		return iPrt;
	}

	/**
	 * Method setJMSPriority
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 */
	public void setJMSPriority(int parm1) throws JMSException {
		iPrt = parm1;
	}

	/**
	 * Method clearProperties
	 *
	 *
	 * @throws JMSException
	 *
	 */
	public void clearProperties() throws JMSException {
		oPrp.clear();
	}

	/**
	 * Method propertyExists
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public boolean propertyExists(String parm1) throws JMSException {
		return oPrp.containsKey(parm1);
	}

	/**
	 * Method getBooleanProperty
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public boolean getBooleanProperty(String parm1) throws JMSException {
		return ((Boolean) oPrp.get(parm1)).booleanValue();
	}

	/**
	 * Method getByteProperty
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public byte getByteProperty(String parm1) throws JMSException {
		return ((Byte) oPrp.get(parm1)).byteValue();
	}

	/**
	 * Method getShortProperty
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public short getShortProperty(String parm1) throws JMSException {
		return ((Short) oPrp.get(parm1)).shortValue();
	}

	/**
	 * Method getIntProperty
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public int getIntProperty(String parm1) throws JMSException {
		return ((Integer) oPrp.get(parm1)).intValue();
	}

	/**
	 * Method getLongProperty
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public long getLongProperty(String parm1) throws JMSException {
		return ((Long) oPrp.get(parm1)).longValue();
	}

	/**
	 * Method getFloatProperty
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public float getFloatProperty(String parm1) throws JMSException {
		return ((Float) oPrp.get(parm1)).floatValue();
	}

	/**
	 * Method getDoubleProperty
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public double getDoubleProperty(String parm1) throws JMSException {
		return ((Double) oPrp.get(parm1)).doubleValue();
	}

	/**
	 * Method getStringProperty
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public String getStringProperty(String parm1) throws JMSException {
		return (String) oPrp.get(parm1);
	}

	/**
	 * Method getObjectProperty
	 *
	 *
	 * @param parm1
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public Object getObjectProperty(String parm1) throws JMSException {
		return oPrp.get(parm1);
	}

	/**
	 * Method getPropertyNames
	 *
	 *
	 * @throws JMSException
	 *
	 * @return
	 *
	 */
	public Enumeration getPropertyNames() throws JMSException {
		return oPrp.keys();
	}

	/**
	 * Method setBooleanProperty
	 *
	 *
	 * @param parm1
	 * @param parm2
	 *
	 * @throws JMSException
	 *
	 */
	public void setBooleanProperty(String parm1, boolean parm2) throws JMSException {
		oPrp.put(parm1, new Boolean(parm2));
	}

	/**
	 * Method setByteProperty
	 *
	 *
	 * @param parm1
	 * @param parm2
	 *
	 * @throws JMSException
	 *
	 */
	public void setByteProperty(String parm1, byte parm2) throws JMSException {
		oPrp.put(parm1, new Byte(parm2));
	}

	/**
	 * Method setShortProperty
	 *
	 *
	 * @param parm1
	 * @param parm2
	 *
	 * @throws JMSException
	 *
	 */
	public void setShortProperty(String parm1, short parm2) throws JMSException {
		oPrp.put(parm1, new Short(parm2));
	}

	/**
	 * Method setIntProperty
	 *
	 *
	 * @param parm1
	 * @param parm2
	 *
	 * @throws JMSException
	 *
	 */
	public void setIntProperty(String parm1, int parm2) throws JMSException {
		oPrp.put(parm1, new Integer(parm2));
	}

	/**
	 * Method setLongProperty
	 *
	 *
	 * @param parm1
	 * @param parm2
	 *
	 * @throws JMSException
	 *
	 */
	public void setLongProperty(String parm1, long parm2) throws JMSException {
		oPrp.put(parm1, new Long(parm2));
	}

	/**
	 * Method setFloatProperty
	 *
	 *
	 * @param parm1
	 * @param parm2
	 *
	 * @throws JMSException
	 *
	 */
	public void setFloatProperty(String parm1, float parm2) throws JMSException {
		oPrp.put(parm1, new Float(parm2));
	}

	/**
	 * Method setDoubleProperty
	 *
	 *
	 * @param parm1
	 * @param parm2
	 *
	 * @throws JMSException
	 *
	 */
	public void setDoubleProperty(String parm1, double parm2) throws JMSException {
		oPrp.put(parm1, new Double(parm2));
	}

	/**
	 * Method setStringProperty
	 *
	 *
	 * @param parm1
	 * @param parm2
	 *
	 * @throws JMSException
	 *
	 */
	public void setStringProperty(String parm1, String parm2) throws JMSException {
		oPrp.put(parm1, parm2);
	}

	/**
	 * Method setObjectProperty
	 *
	 *
	 * @param parm1
	 * @param parm2
	 *
	 * @throws JMSException
	 *
	 */
	public void setObjectProperty(String parm1, Object parm2) throws JMSException {
		oPrp.put(parm1, parm2);
	}

	/**
	 * Method acknowledge
	 *
	 *
	 * @throws JMSException
	 *
	 */
	public void acknowledge() throws JMSException {
		// TODO: Add your code here
	}

	/**
	 * Method clearBody
	 *
	 *
	 * @throws JMSException
	 *
	 */
	public void clearBody() throws JMSException {
		oObj = null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		Table oTbl = null;
		IndexableTable oITbl = null;
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

			if (oDts.getTransactionManager()!=null)
				oDts.getTransactionManager().begin();

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

					/*
					if (null == oTbl) {
						if (DebugFile.trace)
							DebugFile.writeln("opening table " + oRec.getTableName() + " from DataSource " + oDts);
						oTbl = oDts.openIndexedTable(oRec);
					}
					*/

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
							DebugFile.writeln("COMMAND_DELETE_RECORDS " + oRec.getTableName());
						String[] aKeys = getStringProperty("keys").split("`");
						for (int k = aKeys.length; k <= 0; k--) {
							oRec.setKey(aKeys[k]);
							oRec.delete(oDts);
						}
						break;
					} // end switch

					/*
					if (r == nRecs - 1) {
						oTbl.close();
						oTbl = null;
					} else if (!oRec.getTableName().equals(aRecs[r + 1].getTableName())) {
						oTbl.close();
						oTbl = null;
					}
					*/
				}
			} // next

			if (nParams > 0) {
				switch (iCmd) {
				case COMMAND_UPDATE_RECORD:
					if (DebugFile.trace)
						DebugFile.writeln("COMMAND_UPDATE_RECORD FROM " + oRec.getTableName());
					if (DebugFile.trace)
						DebugFile.writeln("opening table " + oRec.getTableName() + " from DataSource " + oDts + " for update");
					oITbl = oDts.openIndexedTable(oRec);
					oITbl.update(aParms, aWhere);
					oITbl.close();
					oITbl = null;
					break;

				case COMMAND_INSERT_RECORD:
					if (DebugFile.trace)
						DebugFile.writeln("COMMAND_INSERT_RECORD INTO " + oRec.getTableName());
					if (DebugFile.trace)
						DebugFile.writeln("opening table " + oRec.getTableName() + " from DataSource " + oDts + " for insert");
					oTbl = oDts.openTable(oRec);
					oTbl.insert(aParms);
					oTbl.close();
					oTbl = null;
					break;
				} // end switch
			}

			if (oDts.getTransactionManager()!=null)
				oDts.getTransactionManager().commit();
			
		} catch (Exception xcpt) {
			if (DebugFile.trace) {
				DebugFile.writeln("ObjectMessageImpl.run() " + xcpt.getClass().getName() + " " + xcpt.getMessage());
				try { DebugFile.writeln(StackTraceUtil.getStackTrace(xcpt)); } catch (IOException ignore) { }
			}
			try {
				if (oDts.getTransactionManager()!=null)
					if (oDts.inTransaction()) oDts.getTransactionManager().rollback();
			} catch (Exception ignore) { }
		} finally {
			try { if (oITbl!= null) oITbl.close();} catch (Exception ignore) { }
			try { if (oTbl != null) oTbl.close(); } catch (Exception ignore) { }
		}
		if (DebugFile.trace)
			DebugFile.writeln("End ObjectMessageImpl.run()");
	}

	public final static int COMMAND_STORE_RECORD = 1;
	public final static int COMMAND_DELETE_RECORDS = 4;
	public final static int COMMAND_INSERT_RECORD = 8;
	public final static int COMMAND_UPDATE_RECORD = 16;

}
