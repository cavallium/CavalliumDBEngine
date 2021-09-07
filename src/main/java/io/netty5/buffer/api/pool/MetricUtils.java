package io.netty5.buffer.api.pool;

import io.netty5.buffer.api.pool.PoolArenaMetric;
import io.netty5.buffer.api.pool.PooledBufferAllocator;
import io.netty5.util.internal.ReflectionUtil;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;

/**
 * Netty5 hides some metrics. This utility class can read them.
 */
public class MetricUtils {

	private static final MethodHandle GET_ARENA_METRICS;

	static {
		var lookup = MethodHandles.lookup();

		// Get the method handle that returns the metrics of each pool arena
		MethodHandle handle = null;
		try {
			// Find the class
			var pooledBufferClass = Class.forName("io.netty5.buffer.api.pool.PooledBufferAllocatorMetric");
			// Find the handle of the method
			handle = lookup.findVirtual(pooledBufferClass, "arenaMetrics", MethodType.methodType(List.class));
		} catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException ignored) {

		}
		GET_ARENA_METRICS = handle;
	}

	/**
	 * Get the metrics of each pool arena of a pooled allocator
	 * @param allocator Pooled allocator
	 * @return A list of {@link PoolArenaMetric}
	 */
	@SuppressWarnings("unchecked")
	public static List<PoolArenaMetric> getPoolArenaMetrics(PooledBufferAllocator allocator) {
		var metric = allocator.metric();
		try {
			// Invoke the method to get the metrics
			return (List<PoolArenaMetric>) GET_ARENA_METRICS.invoke(metric);
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}
}