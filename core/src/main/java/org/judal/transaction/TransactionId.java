package org.judal.transaction;

import java.nio.ByteBuffer;

import javax.transaction.xa.Xid;

public class TransactionId implements Xid {

	private int fid;
	private byte[] gtrid;
	private byte[] bqual;

	public TransactionId() {
		fid = DEFAULT_FORMAT;
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(System.currentTimeMillis());
	    gtrid = buffer.array();
	    buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(Thread.currentThread().getId());
	    bqual = buffer.array();
	}
	
	public TransactionId(int formatId, long globalTransactionId, long branchQualifier) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(globalTransactionId);
	    gtrid = buffer.array();
	    buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(branchQualifier);
	    bqual = buffer.array();
	    fid = formatId;
	}

	@Override
	public byte[] getBranchQualifier() {
		return isNull() ? null : bqual;
	}

	@Override
	public int getFormatId() {
		return fid;
	}

	@Override
	public byte[] getGlobalTransactionId() {
		return isNull() ? null : gtrid;
	}

	@Override
	public String toString() {
		StringBuffer str = new StringBuffer();
		str.append("(");
		str.append(String.valueOf(fid));
		str.append(",");
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.put(gtrid);
		buffer.flip();
		str.append(String.valueOf(buffer.getLong()));
		str.append(",");
		buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.put(bqual);
		buffer.flip();
		str.append(String.valueOf(buffer.getLong()));
		str.append(")");
		return str.toString();
	}

	public boolean isNull() {
		return fid==NO_TRANSACTION_ID;
	}
	
	public static final int DEFAULT_FORMAT = 1;
	public static final int NO_TRANSACTION_ID = -1;
}
