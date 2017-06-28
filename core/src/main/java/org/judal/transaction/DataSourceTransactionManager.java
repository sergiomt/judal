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

/**
 * <p>Implementation of javax.transaction.TransactionManager</p>
 * This class manages transactions per thread.
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class DataSourceTransactionManager implements TransactionManager {

	/**
	 * <p>Default transaction manager.</p>
	 */
	public static DataSourceTransactionManager Transact  = new DataSourceTransactionManager();
			
	private static ThreadLocal<Transaction> threadTransaction = new ThreadLocal<Transaction>();

	private Class<? extends Transaction> transactionClass;
	
	/**
	 * <p>Constructor.</p>
	 * Create new instance of DataSourceTransactionManager using org.judal.transaction.DataSourceTransaction as transaction interface implementation.
	 */
	public DataSourceTransactionManager() {
		this.transactionClass = DataSourceTransaction.class;
	}

	/**
	 * <p>Constructor.</p>
	 * Create new instance of DataSourceTransactionManager using the given transaction implementation.
	 * @param transactionClass Class&lt;? extends Transaction&gt;
	 */
	public DataSourceTransactionManager(Class<? extends Transaction> transactionClass) {
		this.transactionClass = transactionClass;
	}
	
	/**
	 * <p>Get transaction for the current thread (if any).</p>
	 * @return Transaction or <b>null</b>
	 */
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

	/**
	 * <p>Begin transaction./<p>
	 * Create a new instance of the transaction class injected in the constructor (or org.judal.transaction.DataSourceTransaction if the default constructor was used to create this instance).
	 * For the new transaction, call Transaction.begin()
	 * @throws NotSupportedException
	 * @throws SystemException
	 */
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

	/**
	 * <p>Commit active transaction and create a new one for the current thread.</p>
	 * @throws RollbackException
	 * @throws HeuristicMixedException
	 * @throws HeuristicRollbackException
	 * @throws SecurityException
	 * @throws IllegalStateException It there is no current transaction or transaction status is any of {STATUS_NO_TRANSACTION, STATUS_COMMITTED, STATUS_ROLLEDBACK }
	 * @throws SystemException
	 */
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

	/**
	 * @return int Status of the transaction for current thread
	 * @throws SystemException
	 * @throws IllegalStateException if there is no transaction for the current thread
	 */
	@Override
	public int getStatus() throws IllegalStateException, SystemException {
		if (threadTransaction.get()==null)
			throw new IllegalStateException("There is no transaction for the current thread");		
		return threadTransaction.get().getStatus();
	}

	/**
	 * <p>Get transaction for the current thread.</p>
	 * Create a new one on the fly if it did not already exist.
	 * @return Transaction
	 */
	@Override
	public Transaction getTransaction() throws SystemException {
		if (threadTransaction.get()==null)
			setNewTransaction();
		return threadTransaction.get();
	}
	
	/**
	 * <p>Resume transaction.</p>
	 * @param trn Transaction
	 * @throws NullPointerException if trn is <b>null</b>
	 * @throws IllegalStateException if another transaction is already associated with the current thread
	 * @throws InvalidTransactionException
	 * @throws SystemException
	 */
	@Override
	public void resume(Transaction trn) throws NullPointerException, InvalidTransactionException, IllegalStateException, SystemException {
		if (null==trn)
			throw new NullPointerException("DataSourceTransactionManage.resume() Resumed transaction cannot be null");
		if (threadTransaction.get()!=null && getTransaction().getStatus()!=Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException("Another transaction is already associated with the current thread");
		} else {
			threadTransaction.set(trn);
		}
	}

	/**
	 * <p>Rollback transaction for current thread</p>
	 * @throws IllegalStateException if there is no transaction or transaction status is STATUS_NO_TRANSACTION or STATUS_COMMITTED or STATUS_COMMITTING or STATUS_ROLLEDBACK
	 * @throws SecurityException
	 * @throws SystemException
	 */
	@Override
	public void rollback() throws IllegalStateException, SecurityException, SystemException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin DataSourceTransactionManager.rollback()");
			DebugFile.incIdent();
		}

		requireTransaction("rollback");
		if (getStatus()==Status.STATUS_COMMITTING)
			throw new IllegalStateException("Cannot rollback a transaction while it's committing");
		getTransaction().rollback();
		setNewTransaction();

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DataSourceTransactionManager.rollback()");
		}
	}

	/**
	 * <p>Set current thread transaction to rollback only.</p>
	 * If no transaction exists then a new one is created.
	 * @throws IllegalStateException
	 * @throws SystemException
	 */
	@Override
	public void setRollbackOnly() throws IllegalStateException, SystemException {
		requireTransaction("set rollback");
		getTransaction().setRollbackOnly();
	}

	/**
	 * <p>This method does not currently do anything.</p>
	 * @param seconds int
	 * @throws SystemException
	 */
	@Override
	public void setTransactionTimeout(int seconds) throws SystemException {
		requireTransaction("timeout");
	}

	/**
	 * <p>Suspend active transaction and set a new one for the current thread.</p>
	 * @return Transaction Former transaction
	 * @throws IllegalStateException
	 * @throws SystemException
	 */
	@Override
	public Transaction suspend() throws IllegalStateException, SystemException {
		requireTransaction("suspend");
		Transaction suspended = threadTransaction.get();
		setNewTransaction();
		return suspended;
	}

}
