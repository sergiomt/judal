package org.judal.file;

import java.util.concurrent.atomic.AtomicLong;

import javax.jdo.datastore.Sequence;

public class InMemorySequence implements Sequence {

	private AtomicLong currentValue;
	private String sequenceName;
	
	public InMemorySequence(String name, long startWith) {
		sequenceName = name;
		currentValue = new AtomicLong(startWith);
	}
	
	@Override
	public void allocate(int unused) {
	}

	@Override
	public Long current() {
		return currentValue.longValue();
	}

	@Override
	public long currentValue() {
		return currentValue.longValue();
	}

	@Override
	public String getName() {
		return sequenceName;
	}

	@Override
	public Long next() {
		return currentValue.incrementAndGet();
	}

	@Override
	public long nextValue() {
		return currentValue.incrementAndGet();
	}

}
