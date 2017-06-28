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

import java.nio.ByteBuffer;

import javax.transaction.xa.Xid;

/**
 * <p>Implementation of javax.transaction.xa.Xid interface.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class TransactionId implements Xid {
	
	private int fid;
	private byte[] gtrid;
	private byte[] bqual;

	/**
	 * <p>Default Constructor.</p>
	 * Create a new Xid with DEFAULT_FORMAT using System.nanoTime() as transaction id and Thread.currentThread().getId() as branch qualifier
	 */
	public TransactionId() {
		fid = DEFAULT_FORMAT;
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(System.nanoTime());
	    gtrid = buffer.array();
	    buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(Thread.currentThread().getId());
	    bqual = buffer.array();
	}
	
	/**
	 * <p>Constructor.</p>
	 * Create a new Xid with given format, transaction id and branch qualifier
	 * @param formatId int Format Id.
	 * @param globalTransactionId long Transaction Id
	 * @param branchQualifier long Branch Qualifier
	 */
	public TransactionId(int formatId, long globalTransactionId, long branchQualifier) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(globalTransactionId);
	    gtrid = buffer.array();
	    buffer = ByteBuffer.allocate(Long.BYTES);
	    buffer.putLong(branchQualifier);
	    bqual = buffer.array();
	    fid = formatId;
	}

	/**
	 * @return byte[]
	 */
	@Override
	public byte[] getBranchQualifier() {
		return isNull() ? null : bqual;
	}

	/**
	 * @return int
	 */
	@Override
	public int getFormatId() {
		return fid;
	}

	/**
	 * @return byte[]
	 */
	@Override
	public byte[] getGlobalTransactionId() {
		return isNull() ? null : gtrid;
	}

	/**
	 * @return String "(<i>format id</i>, <i>transaction id</i>, <i>branch qualifier</i>)"
	 */
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
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

	/**
	 * @return <b>true</b> if format is NO_TRANSACTION_ID, <b>false</b> otherwise
	 */
	public boolean isNull() {
		return fid==NO_TRANSACTION_ID;
	}
	
	/**
	 * Default Xid format (1)
	 */
	public static final int DEFAULT_FORMAT = 1;

	/**
	 * No Xid format (-1)
	 */
	public static final int NO_TRANSACTION_ID = -1;
}
