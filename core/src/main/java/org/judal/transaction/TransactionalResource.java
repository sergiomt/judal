package org.judal.transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public abstract class TransactionalResource implements XAResource {

	private Xid id;
	private int flags;
	private long created;
	private int timeout;
	private boolean prepared;

	private static ConcurrentHashMap<Xid,LinkedList<TransactionalResource>> resources = new ConcurrentHashMap<Xid,LinkedList<TransactionalResource>>();

	public TransactionalResource() {
		this(new TransactionId());
	}

	public TransactionalResource(Xid tid) {
		id = tid;
		prepared = false;
		timeout = -1;
		created = System.currentTimeMillis();
		LinkedList<TransactionalResource> list;
		if (resources.containsKey(tid)) {
			list = resources.get(tid);
			list.add(this);
		} else {
			list = new LinkedList<TransactionalResource>();
			list.add(this);
			resources.put(tid, list);
		}
	}
	
	protected abstract void startResource(int flags) throws XAException;
	
	protected abstract int prepareResource() throws XAException;

	protected abstract void commitResource() throws XAException;

	protected abstract void rollbackResource() throws XAException;
	
	protected abstract void endResource(int flag) throws XAException;

	public abstract boolean setTransactionTimeout(int seconds);
	
	public Xid getId() {
		return id;
	}

	public List<TransactionalResource> listResourcesForTransaction(Xid tid) throws XAException {
		if (!resources.containsKey(tid))
			resources.put(tid, new LinkedList<TransactionalResource>());
		return Collections.unmodifiableList(resources.get(tid));
	}

	@Override
	public void commit(Xid tid, boolean onePhase) throws XAException {
		commitResource();
		prepared = false;
	}

	@Override
	public void end(Xid tid, int onePhase) throws XAException {
		endResource(onePhase);
		prepared = false;
		if (resources.containsKey(tid))
			resources.get(tid).remove(this);
	}

	@Override
	public void forget(Xid tid) throws XAException {
		if (resources.containsKey(tid))
			resources.get(tid).remove(this);
	}

	@Override
	public int getTransactionTimeout() throws XAException {
		return timeout;
	}

	@Override
	public boolean isSameRM(XAResource res) throws XAException {
		return res instanceof TransactionalResource;
	}

	@Override
	public int prepare(Xid tid) throws XAException {
		prepareResource();
		return XA_OK;
	}

	@Override
	public Xid[] recover(int flag) throws XAException {
		ArrayList<Xid> list = new ArrayList<Xid>();
		for (Entry<Xid,LinkedList<TransactionalResource>> branch : resources.entrySet()) {
			if (branch.getValue().peekFirst().prepared)
				list.add(branch.getKey());
		}
		return list.toArray(new Xid[list.size()]);
	}

	@Override
	public void rollback(Xid tid) throws XAException {
		rollbackResource();
	}

	@Override
	public void start(Xid tid, int flag) throws XAException {
		startResource(flag);
	}

}
