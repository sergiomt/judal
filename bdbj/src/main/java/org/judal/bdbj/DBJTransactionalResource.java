package org.judal.bdbj;

import javax.transaction.Status;
import javax.transaction.xa.XAException;

import org.judal.transaction.TransactionalResource;

import com.knowgate.debug.DebugFile;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;

public class DBJTransactionalResource extends TransactionalResource {

	private Environment bdbEnv;
	private Transaction bdbTransaction;
	private int status;
	
	public DBJTransactionalResource(Environment env) {
		bdbEnv = env;
		bdbTransaction = null;
		status = Status.STATUS_NO_TRANSACTION;
	}
	
	@Override
	protected void startResource(int flags) throws XAException,IllegalStateException {		
		if (DebugFile.trace)
			DebugFile.writeln("Begin DBTransactionalResource.startResource("+String.valueOf(flags)+")");
		if (bdbTransaction!=null) {
			if (DebugFile.trace) DebugFile.writeln("IllegalStateException Transactional resource is already started and has status " + getStatusAsString());
			throw new IllegalStateException("Transactional resource is already started and has status " + getStatusAsString());
		}
		try {
			bdbTransaction = bdbEnv.beginTransaction(null, null);
			status = Status.STATUS_ACTIVE;
		} catch (DatabaseException dbe) {
			throw new XAException(dbe.getMessage());
		}
		if (DebugFile.trace)
			DebugFile.writeln("End DBTransactionalResource.startResource()");
	}

	@Override
	protected int prepareResource() throws XAException {
		if (DebugFile.trace)
			DebugFile.writeln("Begin DBTransactionalResource.prepareResource()");
		if (status != Status.STATUS_ACTIVE) {
			if (DebugFile.trace) DebugFile.writeln("IllegalStateException Transactional resource cannot be prepared because it is in status " + getStatusAsString());
			throw new IllegalStateException("Transactional resource cannot be prepared because it is in status " + getStatusAsString());
		}
		long retval;
		try {
			status = Status.STATUS_PREPARING;
			retval = bdbTransaction.getId();
			status = Status.STATUS_PREPARED;
		} catch (DatabaseException dbe) {
			throw new XAException(dbe.getMessage());
		}
		if (DebugFile.trace)
			DebugFile.writeln("End DBTransactionalResource.prepareResource()");
		return (int) retval;
	}

	@Override
	protected void commitResource() throws XAException {
		if (DebugFile.trace)
			DebugFile.writeln("Begin DBTransactionalResource.commitResource()");
		if (status != Status.STATUS_ACTIVE) {
			if (DebugFile.trace) DebugFile.writeln("IllegalStateException Transactional resource cannot be commited because it is in status " + getStatusAsString());
			throw new IllegalStateException("Transactional resource cannot be commited because it is in status " + getStatusAsString());
		}
		try {
			status = Status.STATUS_COMMITTING;
			bdbTransaction.commit();
			status = Status.STATUS_COMMITTED;
		} catch (DatabaseException dbe) {
			throw new XAException(dbe.getMessage());
		}
		if (DebugFile.trace)
			DebugFile.writeln("End DBTransactionalResource.commitResource()");
	}

	@Override
	protected void rollbackResource() throws XAException {
		if (DebugFile.trace)
			DebugFile.writeln("Begin DBTransactionalResource.rollbackResource()");
		if (status != Status.STATUS_ACTIVE) {
			if (DebugFile.trace) DebugFile.writeln("IllegalStateException Transactional resource cannot be rolled back because it is in status " + getStatusAsString());
			throw new IllegalStateException("Transactional resource cannot be rolled back because it is in status " + getStatusAsString());
		}
		try {
			status = Status.STATUS_ROLLING_BACK;
			bdbTransaction.abort();
			status = Status.STATUS_ROLLEDBACK;
		} catch (DatabaseException dbe) {
			throw new XAException(dbe.getMessage());
		}
		if (DebugFile.trace)
			DebugFile.writeln("End DBTransactionalResource.rollbackResource()");
	}

	@Override
	protected void endResource(int flag) throws XAException,IllegalStateException {
		if (DebugFile.trace)
			DebugFile.writeln("Begin DBTransactionalResource.endResource()");

		if (status==Status.STATUS_NO_TRANSACTION) {
			if (DebugFile.trace) DebugFile.writeln("IllegalStateException The resource was not been started or has been already ended");
			throw new IllegalStateException("DBTransactionalResource.endResource() The resource was not been started or has been already ended");
		}
		
		if (status!=Status.STATUS_ROLLEDBACK && status!=Status.STATUS_COMMITTED) {
			if (DebugFile.trace) DebugFile.writeln("IllegalStateException Cannot end a resource with an active transaction in status "+getStatusAsString());
			throw new IllegalStateException("DBTransactionalResource.endResource() Cannot end a resource with an active transaction in status "+getStatusAsString());
		}

		bdbTransaction = null;
		status = Status.STATUS_NO_TRANSACTION;

		if (DebugFile.trace)			
			DebugFile.writeln("End DBTransactionalResource.endResource()");
	}

	@Override
	public boolean setTransactionTimeout(int seconds) {
		boolean retval;
		try {
			bdbTransaction.setTxnTimeout(1000000L * seconds);
			retval = true;
		} catch (DatabaseException dbe) {
			retval = false;
		}
		return retval;
	}

	public Environment environment() {
		return bdbEnv;
	}

	public Transaction transaction() {
		return bdbTransaction;
	}

	public String getStatusAsString() {
		switch (status) {
		case Status.STATUS_NO_TRANSACTION:
			return "no transaction";
		case Status.STATUS_ACTIVE:
			return "active";
		case Status.STATUS_PREPARING:
			return "preparing";
		case Status.STATUS_PREPARED:
			return "prepared";
		case Status.STATUS_COMMITTING:
			return "committing";
		case Status.STATUS_COMMITTED:
			return "commited";
		case Status.STATUS_MARKED_ROLLBACK:
			return "marked rollback";
		case Status.STATUS_ROLLEDBACK:
			return "rolledback";
		case Status.STATUS_ROLLING_BACK:
			return "rolling back";
		default:
			return "unknown";
		}
	}
}
