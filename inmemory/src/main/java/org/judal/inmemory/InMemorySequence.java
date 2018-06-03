package org.judal.inmemory;

import java.util.concurrent.atomic.AtomicLong;

import javax.jdo.datastore.Sequence;

public class InMemorySequence implements Sequence {

	private AtomicLong value;

	private String name;

	public InMemorySequence(final String name, final long initial) {
		this.name = name;
		this.value = new AtomicLong(initial);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Object next() {
		return nextValue();
	}

	@Override
	public void allocate(int additional) {
	}

	@Override
	public Object current() {
		return currentValue();
	}

	@Override
	public long nextValue() {
		return value.incrementAndGet();
	}

	@Override
	public long currentValue() {
		return value.get();
	}

}
