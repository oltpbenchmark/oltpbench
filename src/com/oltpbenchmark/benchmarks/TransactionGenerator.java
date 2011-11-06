package com.oltpbenchmark.benchmarks;

public interface TransactionGenerator<T extends Operation> {
	/** Implementations *must* be thread-safe. */
	public T nextTransaction();
}
