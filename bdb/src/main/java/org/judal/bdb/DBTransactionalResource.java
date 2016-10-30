package org.judal.bdb;

import javax.transaction.xa.XAException;

import org.judal.transaction.TransactionalResource;

import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Environment;
import com.sleepycat.db.Transaction;

public class DBTransactionalResource extends TransactionalResource {

	private Environment bdbEnv;
	private Transaction bdbTransaction;
	
	public DBTransactionalResource (Environment env) {
		bdbEnv = env;
		bdbTransaction = null;
	}
	
	@Override
	protected void startResource(int flags) throws XAException {
		try {
			bdbTransaction = bdbEnv.beginTransaction(null, null);
		} catch (DatabaseException dbe) {
			throw new XAException(dbe.getMessage());
		}
	}

	@Override
	protected int prepareResource() throws XAException {
		try {
			bdbTransaction.prepare(getId().getGlobalTransactionId());
			return bdbTransaction.getId();
		} catch (DatabaseException dbe) {
			throw new XAException(dbe.getMessage());
		}
	}

	@Override
	protected void commitResource() throws XAException {
		try {
			bdbTransaction.commit();
		} catch (DatabaseException dbe) {
			throw new XAException(dbe.getMessage());
		}
	}

	@Override
	protected void rollbackResource() throws XAException {
		try {
			bdbTransaction.abort();
		} catch (DatabaseException dbe) {
			throw new XAException(dbe.getMessage());
		}
	}

	@Override
	protected void endResource(int flag) throws XAException {
	}

	@Override
	public boolean setTransactionTimeout(int seconds) {
		boolean retval;
		try {
			bdbTransaction.setTxnTimeout(1000000l*seconds);
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
}
