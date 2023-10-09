package it.cavallium.dbengine.client;

import java.util.concurrent.atomic.AtomicLong;

public class LongProgressTracker {

	private final AtomicLong current = new AtomicLong();
	private final AtomicLong total = new AtomicLong();

	public LongProgressTracker(long size) {
		setTotal(size);
	}

	public LongProgressTracker() {

	}

	public LongProgressTracker setTotal(long estimate) {
		total.set(estimate);
		return this;
	}

	public long getCurrent() {
		return current.get();
	}

	public long incrementAndGet() {
		return current.incrementAndGet();
	}

	public long getAndIncrement() {
		return current.getAndIncrement();
	}

	public long getTotal() {
		return Math.max(current.get(), total.get());
	}

	public double progress() {
		return getCurrent() / (double) Math.max(1L, getTotal());
	}
}
