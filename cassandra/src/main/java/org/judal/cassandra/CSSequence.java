package org.judal.cassandra;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.util.ArrayDeque;

import javax.jdo.datastore.Sequence;

import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CounterQuery;

public class CSSequence implements Sequence {
	
	private String sSequenceName;
	private Keyspace oKeySpace;
	private Mutator<String> oMtr;
	private Long currentVal;
	private ArrayDeque<Long> allocated;
	
	public CSSequence(String sSequenceName, Keyspace oKeySpace, Mutator<String> oMtr) {
		this.sSequenceName = sSequenceName;
		this.oKeySpace = oKeySpace;
		this.oMtr = oMtr;
		currentVal = null;
		allocated = null;
	}

	@Override
	public synchronized void allocate(int count) {
		queryCurrent();
		final long lowerBound = currentVal;
		final long upperBound = lowerBound+count;
		oMtr.incrementCounter("kounter", "counters", getName(), count);
		if (null==allocated)
			allocated = new ArrayDeque<Long>(count);
		for (long l=lowerBound+1; l<upperBound; l++)
			allocated.push(l);
	}

	@Override
	public Object current() {
		if (null==currentVal) queryCurrent();
		return currentVal;
	}

	@Override
	public long currentValue() {
		if (null==currentVal) queryCurrent();
		return currentVal.longValue();
	}

	@Override
	public String getName() {
		return sSequenceName;
	}

	@Override
	public Object next() {
		nextValue();
		return currentVal;
	}

	@Override
	public synchronized long nextValue() {
		boolean preallocated = false;
		if (allocated!=null) {
			if (allocated.size()>0) {
				currentVal = allocated.pop();
				preallocated = true;
			}
		}
		if (!preallocated) {
			oMtr.incrementCounter("kounter", "counters", getName(), 1l);
			queryCurrent();
		}
		return currentVal.longValue();
	}

	private void queryCurrent() {
		CounterQuery<String, String> oQry = new ThriftCounterColumnQuery<String,String>(oKeySpace, StringSerializer.get(), StringSerializer.get());
		oQry.setColumnFamily("counters").setKey("kounter").setName(getName());
		currentVal = new Long(oQry.execute().get().getValue().longValue());		
	}
}
