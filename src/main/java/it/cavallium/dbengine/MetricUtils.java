package it.cavallium.dbengine;

import io.netty5.buffer.api.pool.PoolArenaMetric;
import io.netty5.buffer.api.pool.PooledBufferAllocator;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Netty5 hides some metrics. This utility class can read them.
 */
public class MetricUtils {

	private static final Logger LOG = LogManager.getLogger(MetricUtils.class);
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
		} catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException ex) {
			logMetricsNotAccessible(ex);
		}
		GET_ARENA_METRICS = handle;
	}

	private static void logMetricsNotAccessible(Throwable ex) {
		LOG.debug("Failed to open pooled buffer allocator metrics", ex);
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
			return List.of();
		}
	}
}
