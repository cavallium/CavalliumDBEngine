package io.netty5.buffer.api.pool;

import java.util.List;

public class PooledBufferAllocatorMetricUtils implements BufferAllocatorMetric {

	private final PooledBufferAllocator allocator;

	@SuppressWarnings("RedundantThrows")
	public PooledBufferAllocatorMetricUtils(PooledBufferAllocator allocator) throws Throwable {
		this.allocator = allocator;
	}

	/**
	 * Return the number of arenas.
	 */
	public int numArenas() {
		return allocator.numArenas();
	}

	/**
	 * Return a {@link List} of all {@link PoolArenaMetric}s that are provided by this pool.
	 */
	public List<PoolArenaMetric> arenaMetrics() {
		return allocator.arenaMetrics();
	}

	/**
	 * Return the number of thread local caches used by this {@link PooledBufferAllocator}.
	 */
	public int numThreadLocalCaches() {
		return allocator.numThreadLocalCaches();
	}

	/**
	 * Return the size of the small cache.
	 */
	public int smallCacheSize() {
		return allocator.smallCacheSize();
	}

	/**
	 * Return the size of the normal cache.
	 */
	public int normalCacheSize() {
		return allocator.normalCacheSize();
	}

	/**
	 * Return the chunk size for an arena.
	 */
	public int chunkSize() {
		return allocator.chunkSize();
	}

	@Override
	public long usedMemory() {
		return allocator.usedMemory();
	}
}
