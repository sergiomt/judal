package org.judal.transaction;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;

import com.knowgate.debug.DebugFile;

public class DataSourceTransactionManager implements TransactionManager {

	public static DataSourceTransactionManager Transact  = new DataSourceTransactionManager();
			
	private static ThreadLocal<Transaction> threadTransaction = new ThreadLocal<Transaction>();

	private Class<? extends Transaction> transactionClass;
	
	public DataSourceTransactionManager() {
		this.transactionClass = DataSourceTransaction.class;
	}

	public DataSourceTransactionManager(Class<? extends Transaction> transactionClass) {
		this.transactionClass = transactionClass;
	}
	
	public static Transaction currentTransaction() {
		return threadTransaction.get();
	}
	
	private void setNewTransaction() throws SystemException {
		try {
			threadTransaction.set(transactionClass.newInstance());
		} catch (InstantiationException | IllegalAccessException xcpt) {
			throw new SystemException(xcpt.getMessage());
		}		
	}

	private void requireTransaction(String action) throws IllegalStateException, SystemException {
		if (threadTransaction.get()==null)
			throw new IllegalStateException("There is no active transaction to " + action);
		else if (threadTransaction.get().getStatus()==Status.STATUS_NO_TRANSACTION || 
				 threadTransaction.get().getStatus()==Status.STATUS_COMMITTED ||
				 threadTransaction.get().getStatus()==Status.STATUS_ROLLEDBACK)
			throw new IllegalStateException("There is no active transaction to " + action);
	}

	@Override
	public void begin() throws NotSupportedException, SystemException {
		if (threadTransaction.get()==null)
			setNewTransaction();
		try {
			if (threadTransaction.get() instanceof DataSourceTransaction)
				((DataSourceTransaction) threadTransaction.get()).begin();
		} catch (IllegalStateException | XAException e) {
			throw new SystemException(e.getMessage());
		}
	}

	@Override
	public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, SystemException {	
		
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DataSourceTransactionManager.commit()");
			DebugFile.incIdent();
		}
		
		requireTransaction("commit");
		getTransaction().commit();
		setNewTransaction();

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DataSourceTransactionManager.commit()");
		}
	}

	@Override
	public int getStatus() throws SystemException {
		return getTransaction().getStatus();
	}

	@Override
	public Transaction getTransaction() throws SystemException {
		if (threadTransaction.get()==null)
			setNewTransaction();
		return threadTransaction.get();
	}
	
	@Override
	public void resume(Transaction trn) throws InvalidTransactionException, IllegalStateException, SystemException {
		if (getTransaction().getStatus()!=Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("Another transaction is already associated with the current thread");
		} else {
			setNewTransaction();
		}
	}

	@Override
	public void rollback() throws IllegalStateException, SecurityException, SystemException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DataSourceTransactionManager.rollback()");
			DebugFile.incIdent();
		}

		requireTransaction("rollback");
		getTransaction().rollback();
		setNewTransaction();

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DataSourceTransactionManager.rollback()");
		}
	}

	@Override
	public void setRollbackOnly() throws IllegalStateException, SystemException {
		requireTransaction("set rollback");
		getTransaction().setRollbackOnly();
	}

	@Override
	public void setTransactionTimeout(int seconds) throws SystemException {
		requireTransaction("timeout");
	}

	@Override
	public Transaction suspend() throws SystemException {
		requireTransaction("suspend");
		Transaction suspended = threadTransaction.get();
		setNewTransaction();
		return suspended;
	}

}
