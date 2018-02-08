package org.judal.transaction;

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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.Constants;
import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;

import javax.transaction.xa.Xid;

import com.knowgate.debug.DebugFile;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

/**
 * <p>javax.transaction.Transaction interface implementation.</p>
 * @author Sergio Montoro Ten
 * @version 
 */

public class DataSourceTransaction implements AutoCloseable, javax.transaction.Transaction {

	private Xid tid;
	private int status;
	private long thid;
	private Synchronization sync;
	private LinkedList<XAResource> resources;

	/**
	 * <p>Constructoor.</p>
	 * Create a new Transaction with no resources an STATUS_NO_TRANSACTION
	 */
	public DataSourceTransaction() {
		tid = new TransactionId();
		thid = Thread.currentThread().getId();
		sync = null;
		status = Status.STATUS_NO_TRANSACTION;
		resources = new LinkedList<XAResource>();
		if (DebugFile.trace) DebugFile.writeln("new DataSourceTransaction with Xid = "+tid.toString() + " for thread " + thid);
	}

	/**
	 * @return <b>true</b> if Status is not STATUS_NO_TRANSACTION or STATUS_COMMITTED or STATUS_ROLLEDBACK or STATUS_UNKNOWN
	 */
	public boolean isActive() {
		return status!=Status.STATUS_NO_TRANSACTION && status!=Status.STATUS_COMMITTED && status!=Status.STATUS_ROLLEDBACK && status!=Status.STATUS_UNKNOWN;
	}
	
	/**
	 * <p>Begin transaction.</p>
	 * Set Transaction Status to STATUS_ACTIVE and start every enlisted resource with XAResource.TMJOIN flag
	 * @throws IllegalStateException if transaction isActive or Status is STATUS_UNKNOWN
	 * @throws XAException
	 */
	public void begin() throws IllegalStateException, XAException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DataSourceTransaction.begin()");
			DebugFile.incIdent();
			DebugFile.writeln("Xid = " + tid.toString());
		}

		if (status==Status.STATUS_UNKNOWN) {
			if (DebugFile.trace) {
				DebugFile.writeln("IllegalStateException Transaction "+tid.toString()+" status is unknown");
				DebugFile.decIdent();
			}
			throw new IllegalStateException("Transaction "+tid.toString()+" status is unknown");
		}
		if (isActive()) {
			if (DebugFile.trace) {
				DebugFile.writeln("Transaction "+tid.toString()+" has been already activated");
				DebugFile.decIdent();
			}
			throw new IllegalStateException("IllegalStateException Transaction "+tid.toString()+" has been already activated");
		}
		
		for (XAResource res : resources) {
			res.start(tid, XAResource.TMJOIN);
		}

		status = Status.STATUS_ACTIVE;

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DataSourceTransaction.begin() : " + tid.toString());
		}
	}

	/**
	 * <p>Close transaction.</p>
	 * Set Transaction Status to STATUS_NO_TRANSACTION and end every enlisted resource with XAResource.TMJOIN flag
	 * @throws XAException
	 */
	@Override
	public void close() throws XAException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DataSourceTransaction.close()");
			DebugFile.incIdent();
			DebugFile.writeln("Xid = " + tid.toString());
		}

		while (!resources.isEmpty())
			resources.pollFirst().end(tid, XAResource.TMJOIN);
		status = Status.STATUS_NO_TRANSACTION;

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DataSourceTransaction.close() : " + tid.toString());
		}
	}

	/**
	 * @return long
	 */
	public long getThreadId() {
		return thid;
	}

	/**
	 * @return Synchronization
	 */
	public Synchronization getSynchronization() {
		return sync;
	}

	/**
	 * <p>Commit transaction.</p>
	 * <ol>
	 * <li>Put Transaction in STATUS_COMMITTING</li>
	 * <li>If Synchronization is set Then call Synchronization.beforeCompletion()</li>
	 * <li>For each enlisted resource call XAResource.commit() followed by XAResource.end()</li>
	 * <li>If everything is successful put the Transaction in STATUS_COMMITTED</li>
	 * <li>A failure to commit or end any of the resources will result in a JDOException and the Transaction will be left in STATUS_COMMITTING</li>
	 * <li>If Synchronization is set Then call Synchronization.afterCompletion()</li>
	 * </ol>
	 * @throws IllegalStateException if Status is not STATUS_ACTIVE or STATUS_PREPARED
	 * @throws JDOException This exception will be thrown wrapping javax.transaction.xa.XAException therefore getCause().errorCode on the raised exception will contain an exception code indicating the cause
	 */
	@Override
	public void commit() throws IllegalStateException,JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DataSourceTransaction.commit()");
			DebugFile.incIdent();
			DebugFile.writeln("Xid = " + tid.toString());
		}

		if (status!=Status.STATUS_ACTIVE && status!=Status.STATUS_PREPARED) {
			if (DebugFile.trace) {
				DebugFile.writeln("IllegalStateException Invalid status for commit "+ getStatusAsString() + " at transaction " + tid.toString());
				DebugFile.decIdent();
			}
			throw new IllegalStateException("Invalid status for commit " + getStatusAsString() + " at transaction " + tid.toString());
		}

		try {
			if (sync!=null)
				sync.beforeCompletion();
			status = Status.STATUS_COMMITTING;
			while (!resources.isEmpty()) {
				XAResource res = resources.pollFirst();
				if (DebugFile.trace) DebugFile.writeln("XAResource.commit("+tid.toString()+")");
				res.commit(tid, true);
				res.end(tid, XAResource.TMJOIN);
			}
			status = Status.STATUS_COMMITTED;
		} catch (XAException xcpt) {
			if (DebugFile.trace) DebugFile.decIdent();
			switch (xcpt.errorCode) {
			case XAException.XA_HEURCOM:
			case XAException.XA_HEURHAZ:
				throw new JDOException(xcpt.getMessage(), xcpt);
			case XAException.XA_HEURRB:
				throw new JDOException(xcpt.getMessage(), xcpt);
			case XAException.XA_HEURMIX:
				throw new JDOException(xcpt.getMessage(), xcpt);
			case XAException.XA_RBCOMMFAIL:
			case XAException.XA_RBROLLBACK:
			case XAException.XA_RBOTHER:
				throw new JDOException(xcpt.getMessage(), xcpt);
			default:
				throw new JDOException(xcpt.getMessage(), xcpt);
			}
		}
		if (sync!=null)
			sync.afterCompletion(status);

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DataSourceTransaction.commit() " + tid.toString());
		}
	}

	/**
	 * @return Unmodifiable List&lt;XAResource&gt;
	 */
	public List<XAResource> listResources() {
		return Collections.unmodifiableList(resources);
	}

	/**
	 * <p>Delist resource from transaction.</p>
	 * Call XAResource.end() and remove the resource from the list of resources enlisted with this transaction.
	 * @param res XAResource
	 * @param flag int This parameter is not used
	 * @return boolean <b>true</b> if this transaction had the given resource enlisted, <b>false</b> if this transaction is not enlisted to the resource
	 * @throws IllegalStateException if Transaction Status is STATUS_NO_TRANSACTION or getFormatId() is TransactionId.NO_TRANSACTION_ID
	 * @throws NullPointerException if resource is <b>null</b>
	 * @throws SystemException
	 */
	@Override
	public boolean delistResource(XAResource res, int flag) throws NullPointerException, IllegalStateException, SystemException {
		if (status==Status.STATUS_NO_TRANSACTION) {
			if (DebugFile.trace) DebugFile.writeln("IllegalStateException DataSourceTransaction.delistResource() Current thread has no active transaction");
			throw new IllegalStateException("Current thread has no active transaction");
		}

		if (null==res) {
			if (DebugFile.trace) DebugFile.writeln("NullPointerException DataSourceTransaction.delistResource() XAResource cannot be null");
			throw new NullPointerException("XAResource cannot be null");			
		}

		boolean retval;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DataSourceTransaction.delistResource("+res+","+flag+")");
			DebugFile.incIdent();
		}

		if (contains(res)) {
			if (tid.getFormatId()==TransactionId.NO_TRANSACTION_ID) {
				if (DebugFile.trace) {
					DebugFile.writeln("IllegalStateException Invalid transaction id format");
					DebugFile.decIdent();
				}
				throw new IllegalStateException("Invalid transaction id format");
			} else if (DebugFile.trace) {
				DebugFile.writeln("Xid = " + tid.toString());
			}

			try {
				res.end(tid, XAResource.TMJOIN);
			} catch (XAException xcpt) {
				throw new SystemException(xcpt.getMessage());
			}
			resources.remove(res);
			retval = true;
		} else {
			retval = false;
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DataSourceTransaction.delistResource()");
		}
		
		return retval;
	}

	private boolean contains(XAResource res){
		if (res instanceof TransactionalResource) {
			TransactionalResource trs = (TransactionalResource) res;
			for (XAResource xrs : resources) {
				if (xrs instanceof TransactionalResource) {
					if (trs.getId().equals(((TransactionalResource) xrs).getId())) {
						return true;
					}
				} else {
					if (trs.equals(xrs))
						return true;
				}
			}			
		} else {
			for (XAResource xrs : resources) {
				if (res.equals(xrs))
					return true;
			}
		}
		return false;
	}

	/**
	 * <p>Enlist resource.</p>
	 * Enlist a resource to this transaction and start it with XAResource.TMJOIN.
	 * If resource is already enlisted then nothing is done.
	 * @param res XAResource
	 * @throws NullPointerException if resource is <b>null</b>
	 * @throws RollbackException if Transaction Status is STATUS_MARKED_ROLLBACK or STATUS_ROLLING_BACK
	 * @throws IllegalStateException if getFormatId() is TransactionId.NO_TRANSACTION_ID or Transaction Status is STATUS_PREPARED
	 * @throws SystemException if there is an exception during resource start
	 */
	@Override
	public boolean enlistResource(XAResource res) throws NullPointerException, RollbackException, IllegalArgumentException, IllegalStateException, SystemException {
		
		boolean retval;

		if (null==res) {
			if (DebugFile.trace) DebugFile.writeln("NullPointerException DataSourceTransaction.enlistResource() XAResource cannot be null");
			throw new NullPointerException("XAResource cannot be null");			
		}
		
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DataSourceTransaction.enlistResource("+res+")");
			DebugFile.incIdent();
		}

		if (status==Status.STATUS_MARKED_ROLLBACK || status==Status.STATUS_ROLLING_BACK) {
			if (DebugFile.trace) {
				DebugFile.writeln("RollbackException Cannot enlist resource because transaction was set to rollback only");
				DebugFile.decIdent();
			}			
			throw new RollbackException("Cannot enlist resource because transaction was set to rollback only");
		}
		
		if (status==Status.STATUS_PREPARED) {
			if (DebugFile.trace) {
				DebugFile.writeln("IllegalStateException Cannot enlist resources in prepared transaction " + tid.toString());
				DebugFile.decIdent();
			}
			throw new IllegalStateException("Cannot enlist resources in a prepared transaction " + tid.toString());
		}

		if (!contains(res)) {
			
			if (tid.getFormatId()==TransactionId.NO_TRANSACTION_ID) {
				if (DebugFile.trace) {
					DebugFile.writeln("IllegalStateException Invalid transaction id format");
					DebugFile.decIdent();
				}
				throw new IllegalStateException("Invalid transaction id format");
			}
			
			try {
				if (DebugFile.trace)
					DebugFile.writeln("XAResource.start("+tid.toString()+","+XAResource.TMJOIN+")");
				res.start(tid, XAResource.TMJOIN);
			} catch (XAException xcpt) {
				throw new SystemException(xcpt.getMessage());
			}
			resources.add(res);
			retval = true;
		} else {
			if (DebugFile.trace)
				DebugFile.writeln("DataSourceTransaction " + tid.toString() + " resources already contains resource " + res);
			retval = false;
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DataSourceTransaction.enlistResource() : " + retval);
		}
		
		return retval;
	}

	/**
	 * @return int One of {STATUS_NO_TRANSACTION, STATUS_ACTIVE, STATUS_PREPARING, STATUS_PREPARED, STATUS_COMMITTING, STATUS_COMMITTED, STATUS_MARKED_ROLLBACK, STATUS_ROLLEDBACK, STATUS_ROLLING_BACK }
	 */
	@Override
	public int getStatus() throws SystemException {
		return status;
	}

	/**
	 * <p>Get text describing this transaction status.</p>
	 * <table summary="Transaction status text for int code">
	 * <tr><th>Status Code</th><th>Text</th></tr>
	 * <tr><td>STATUS_NO_TRANSACTION</td><td>no transaction</td></tr>
	 * <tr><td>STATUS_ACTIVE</td><td>active</td></tr>
	 * <tr><td>STATUS_PREPARING</td><td>preparing</td></tr>
	 * <tr><td>STATUS_PREPARED</td><td>prepared</td></tr>
	 * <tr><td>STATUS_COMMITTING</td><td>committing</td></tr>
	 * <tr><td>STATUS_COMMITTED</td><td>committed</td></tr>
	 * <tr><td>STATUS_MARKED_ROLLBACK</td><td>marked rollback</td></tr>
	 * <tr><td>STATUS_ROLLEDBACK</td><td>rolledback</td></tr>
	 * <tr><td>STATUS_ROLLING_BACK</td><td>rolling back</td></tr>
	 * </table>
	 * @return String
	 */
	public String getStatusAsString() {
		return TransactionalResource.getStatusAsString(status);
	}
	
	/**
	 * @param sync Synchronization
	 * @throws RollbackException If Status is STATUS_MARKED_ROLLBACK  or STATUS_ROLLING_BACK
	 * @throws IllegalStateException Is Status is STATUS_NO_TRANSACTION or STATUS_PREPARING or STATUS_PREPARED
	 * @throws SystemException
	 */
	@Override
	public void registerSynchronization(Synchronization sync)
			throws RollbackException, IllegalStateException, SystemException {
		if (status==Status.STATUS_MARKED_ROLLBACK || status==Status.STATUS_ROLLING_BACK)
			throw new RollbackException("Cannot register synchronization because transaction was set to rollback only");
		if (status==Status.STATUS_NO_TRANSACTION || status==Status.STATUS_PREPARING || status==Status.STATUS_PREPARED)
			throw new IllegalStateException("Invalid transaction status " + getStatusAsString());
		this.sync = sync;
	}

	/**
	 * <p>Rollback transaction.</p>
	 * <ol>
	 * <li>Set Status to STATUS_ROLLING_BACK</li>
	 * <li>For each enlisted resource, call XAResource.rollback() followed by XAResource.end(TMJOIN)</li>
	 * <li>Set Status to STATUS_ROLLEDBACK</li>
	 * <li>If Synchronization is set Then call Synchronization.afterCompletion()</li>
	 * </ol>
	 * @throws IllegalStateException Is Status is not STATUS_ACTIVE or STATUS_PREPARED or STATUS_MARKED_ROLLBACK
	 * @throws JDOException
	 */
	@Override
	public void rollback() throws JDOException {
		if (status!=Status.STATUS_ACTIVE && status!=Status.STATUS_PREPARED && status!=Status.STATUS_MARKED_ROLLBACK)
			throw new IllegalStateException("Invalid transaction status for rollback " + getStatusAsString());
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DataSourceTransaction.rollback()");
			DebugFile.incIdent();
			DebugFile.writeln("Xid = "+tid.toString());
		}
		try {
			status = Status.STATUS_ROLLING_BACK;
			while (!resources.isEmpty()) {
				XAResource res = resources.pollFirst();
				if (DebugFile.trace) DebugFile.writeln("XAResource.rollback("+tid.toString()+")");
				res.rollback(tid);
				res.end(tid, XAResource.TMJOIN);
			}
			status = Status.STATUS_ROLLEDBACK;
		} catch (XAException xcpt) {
			if (DebugFile.trace) {
				DebugFile.writeln("XAException " + xcpt.getMessage());
				DebugFile.decIdent();
			}
			throw new JDOException(xcpt.getMessage());
		}		
		if (sync!=null)
			sync.afterCompletion(status);
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DataSourceTransaction.rollback()");
		}
	}

	/**
	 * <p>Set Status to STATUS_MARKED_ROLLBACK</p>
	 */
	@Override
	public void setRollbackOnly() {
		status = Status.STATUS_MARKED_ROLLBACK;
	}

	/**
	 * @return boolean <b>true</b> if this transaction status is STATUS_MARKED_ROLLBACK, <b>false</b> otherwise
	 */
	public boolean getRollbackOnly() {
		return status == Status.STATUS_MARKED_ROLLBACK;
	}
	
	private boolean supportsIsolationLevel() {
		boolean supportIsolationLevel = true;
		if (resources.size()>0) {
			for (XAResource res : resources) {
				supportIsolationLevel = res instanceof Connection;
				if (!supportIsolationLevel)
					break;
			}				
		}
		return supportIsolationLevel;		
	}

	/**
	 * @return String One of javax.jdo.Constants PROPERTY_TRANSACTION_ISOLATION_LEVEL_SNAPSHOT, PROPERTY_TRANSACTION_ISOLATION_LEVEL_READ_UNCOMMITTED, PROPERTY_TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED, PROPERTY_TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ, PROPERTY_TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE
	 * @throws JDOUnsupportedOptionException
	 */
	public String getIsolationLevel() throws JDOUnsupportedOptionException {
		if (!supportsIsolationLevel()) {
			throw new JDOUnsupportedOptionException(resources.getFirst().getClass().getName()+" does not support transaction isolation level");
		} else {
			if (resources.isEmpty())
				return Constants.PROPERTY_TRANSACTION_ISOLATION_LEVEL_SNAPSHOT;
			Connection conn = (Connection) resources.getFirst();
			try {
				switch (conn.getTransactionIsolation()) {
				case Connection.TRANSACTION_READ_UNCOMMITTED:
					return Constants.PROPERTY_TRANSACTION_ISOLATION_LEVEL_READ_UNCOMMITTED;
				case Connection.TRANSACTION_READ_COMMITTED:
					return Constants.PROPERTY_TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED;
				case Connection.TRANSACTION_REPEATABLE_READ:
					return Constants.PROPERTY_TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ;
				case Connection.TRANSACTION_SERIALIZABLE:
					return Constants.PROPERTY_TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE;
				default:
					return Constants.PROPERTY_TRANSACTION_ISOLATION_LEVEL_SNAPSHOT;
				}
			} catch (SQLException sqle) {
				throw new JDOUnsupportedOptionException(sqle.getMessage(), sqle);
			}
		}
	}

	/**
	 * @return String One of javax.jdo.Constants PROPERTY_TRANSACTION_ISOLATION_LEVEL_SNAPSHOT, PROPERTY_TRANSACTION_ISOLATION_LEVEL_READ_UNCOMMITTED, PROPERTY_TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED, PROPERTY_TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ, PROPERTY_TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE
	 */
	public void setIsolationLevel(String level) {
		if (!supportsIsolationLevel()) {
			throw new JDOUnsupportedOptionException(resources.getFirst().getClass().getName()+" does not support transaction isolation level");
		} else {
			try {
				for (XAResource res : resources) {
					@SuppressWarnings("resource")
					Connection conn = (Connection) res;
					if (level.equals(Constants.PROPERTY_TRANSACTION_ISOLATION_LEVEL_READ_UNCOMMITTED))
						conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
					else if (level.equals(Constants.PROPERTY_TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED))
						conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
					else if (level.equals(Constants.PROPERTY_TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ))
						conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
					else if (level.equals(Constants.PROPERTY_TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE))
						conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
				}
			} catch (SQLException sqle) {
				throw new JDOUnsupportedOptionException(sqle.getMessage(), sqle);				
			}
		}
	}
	
	@Override
	public String toString() {
		return "Xid=" + tid.toString() + " Thread=" + thid;
	}
	
}
