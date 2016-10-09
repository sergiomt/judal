package org.judal.transaction;

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

public class DataSourceTransaction implements AutoCloseable, javax.transaction.Transaction {

	private Xid tid;
	private int status;
	private long thid;
	private Synchronization sync;
	private LinkedList<XAResource> resources;

	public DataSourceTransaction() {
		tid = new TransactionId();
		thid = Thread.currentThread().getId();
		sync = null;
		status = Status.STATUS_NO_TRANSACTION;
		resources = new LinkedList<XAResource>();
	}

	public boolean isActive() {
		return status!=Status.STATUS_NO_TRANSACTION && status!=Status.STATUS_COMMITTED && status!=Status.STATUS_ROLLEDBACK && status!=Status.STATUS_UNKNOWN;
	}
	
	public void begin() throws IllegalStateException, XAException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DataSourceTransaction.begin()");
			DebugFile.incIdent();
		}
		if (status==Status.STATUS_UNKNOWN)
			throw new IllegalStateException("Transaction status is unknown");
		if (isActive())
			throw new IllegalStateException("Transaction has been already activated");
		for (XAResource res : resources)
			res.start(tid, XAResource.TMJOIN);
		status = Status.STATUS_ACTIVE;
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DataSourceTransaction.begin()");
		}
	}

	@Override
	public void close() throws XAException {
		while (!resources.isEmpty())
			resources.pollFirst().end(tid, XAResource.TMJOIN);
		status = Status.STATUS_NO_TRANSACTION;
	}

	public long getThreadId() {
		return thid;
	}

	public Synchronization getSynchronization() {
		return sync;
	}

	@Override
	public void commit() throws JDOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DataSourceTransaction.commit()");
			DebugFile.incIdent();
		}
		if (status!=Status.STATUS_ACTIVE && status!=Status.STATUS_PREPARED)
			throw new IllegalStateException("Invalid transaction status for commit "+String.valueOf(status));
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
			DebugFile.writeln("End DataSourceTransaction.commit(");
		}
	}

	public List<XAResource> listResources() {
		return Collections.unmodifiableList(resources);
	}

	@Override
	public boolean delistResource(XAResource res, int flag) throws IllegalStateException, SystemException {
		if (status==Status.STATUS_NO_TRANSACTION)
			throw new IllegalStateException("Current thread has no active transaction");
		if (resources.contains(res)) {
			if (tid.getFormatId()==TransactionId.NO_TRANSACTION_ID)
				throw new IllegalStateException("Invalid transaction id format");
			try {
				res.end(tid, XAResource.TMJOIN);
			} catch (XAException xcpt) {
				throw new SystemException(xcpt.getMessage());
			}
			resources.remove(res);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean enlistResource(XAResource res) throws RollbackException, IllegalStateException, SystemException {
		if (DebugFile.trace)
			DebugFile.writeln("DataSourceTransaction.enlistResource("+res+")");
		if (status==Status.STATUS_MARKED_ROLLBACK)
			throw new RollbackException("Cannot enlist resource because transaction was set to rollback only");
		if (status==Status.STATUS_PREPARED)
			throw new IllegalStateException("Cannot enlist resources in a prepared transaction");
		if (!resources.contains(res)) {
			if (tid.getFormatId()==TransactionId.NO_TRANSACTION_ID)
				throw new IllegalStateException("Invalid transaction id format");
			try {
				res.start(tid, XAResource.TMJOIN);
			} catch (XAException xcpt) {
				throw new SystemException(xcpt.getMessage());
			}
			resources.add(res);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public int getStatus() throws SystemException {
		return status;
	}

	@Override
	public void registerSynchronization(Synchronization sync)
			throws RollbackException, IllegalStateException, SystemException {
		if (status==Status.STATUS_MARKED_ROLLBACK)
			throw new RollbackException("Cannot register synchronization because transaction was set to rollback only");
		if (status==Status.STATUS_NO_TRANSACTION || status==Status.STATUS_PREPARED)
			throw new IllegalStateException("Invalid transaction status "+String.valueOf(status));
		this.sync = sync;
	}

	@Override
	public void rollback() throws JDOException {
		if (status!=Status.STATUS_ACTIVE && status!=Status.STATUS_PREPARED && status!=Status.STATUS_COMMITTING && status!=Status.STATUS_MARKED_ROLLBACK)
			throw new IllegalStateException("Invalid transaction status for rollback "+String.valueOf(status));
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DataSourceTransaction.rollback()");
			DebugFile.incIdent();
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

	@Override
	public void setRollbackOnly() {
		status = Status.STATUS_MARKED_ROLLBACK;
	}

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

}