package it.cavallium.dbengine.netty;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.pool.BufferAllocatorMetric;
import io.netty5.buffer.api.pool.BufferAllocatorMetricProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public class NettyMetrics implements MeterBinder {

	private final BufferAllocator allocator;
	private final String resourceName;
	private final String allocatorName;
	private final Map<String, String> extraTags;

	public NettyMetrics(BufferAllocator allocator,
			String resourceName,
			String allocatorName,
			Map<String, String> extraTags) {
		this.allocator = allocator;
		this.resourceName = resourceName;
		this.allocatorName = allocatorName;
		this.extraTags = extraTags;
	}

	@Override
	public void bindTo(@NotNull MeterRegistry registry) {
		var direct = allocator.getAllocationType().isDirect();
		var pooling = allocator.isPooling();
		List<Tag> tags = new ArrayList<>();
		tags.add(Tag.of("resource", resourceName));
		tags.add(Tag.of("allocator", allocatorName));
		tags.add(Tag.of("type", direct ? "direct" : "heap"));
		tags.add(Tag.of("pooling-mode", pooling ? "pooled" : "unpooled"));
		extraTags.forEach((key, value) -> tags.add(Tag.of(key, value)));

		if (allocator instanceof BufferAllocatorMetricProvider metricProvider) {
			var metric = metricProvider.metric();
			registry.gauge("netty.chunk.size", tags, metric, BufferAllocatorMetric::chunkSize);
			registry.gauge("netty.num.arenas", tags, metric, BufferAllocatorMetric::numArenas);
			registry.gauge("netty.num.thread.local.caches", tags, metric, BufferAllocatorMetric::numThreadLocalCaches);
			registry.gauge("netty.small.cache.size", tags, metric, BufferAllocatorMetric::smallCacheSize);
			registry.gauge("netty.normal.cache.size", tags, metric, BufferAllocatorMetric::normalCacheSize);
		}
	}

}
