package it.cavallium.dbengine.netty;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.pool.BufferAllocatorMetric;
import io.netty5.buffer.api.pool.BufferAllocatorMetricProvider;
import io.netty5.buffer.api.pool.PoolArenaMetric;
import io.netty5.buffer.api.pool.PoolChunkListMetric;
import io.netty5.buffer.api.pool.PoolSubpageMetric;
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
			registry.gauge("netty.num.arenas", tags, metric, BufferAllocatorMetric::numArenas);
			int arenaId = 0;
			for (PoolArenaMetric arenaMetric : metric.arenaMetrics()) {
				var currentArenaId = arenaId;
				var arenaTags = new ArrayList<>(tags);
				arenaTags.add(Tag.of("arena.offset", String.valueOf(currentArenaId)));
				registry.gauge("netty.arena.thread.caches.num", arenaTags, arenaMetric, PoolArenaMetric::numThreadCaches);
				registry.gauge("netty.arena.subpage.num", tagsWithSize(arenaTags, "small"), arenaMetric, PoolArenaMetric::numSmallSubpages);
				registry.gauge("netty.arena.chunk.lists.num", arenaTags, arenaMetric, PoolArenaMetric::numChunkLists);
				registerSubpageMetrics(registry, arenaTags, arenaMetric.smallSubpages(), "small");
				registerPoolChunkMetrics(registry, arenaTags, arenaMetric.chunkLists());
				registry.gauge("netty.arena.allocations.num", tagsWithSize(arenaTags, "small"), arenaMetric, PoolArenaMetric::numSmallAllocations);
				registry.gauge("netty.arena.allocations.num", tagsWithSize(arenaTags, "normal"), arenaMetric, PoolArenaMetric::numNormalAllocations);
				registry.gauge("netty.arena.allocations.num", tagsWithSize(arenaTags, "huge"), arenaMetric, PoolArenaMetric::numHugeAllocations);
				registry.gauge("netty.arena.deallocations.num", tagsWithSize(arenaTags, "small"), arenaMetric, PoolArenaMetric::numSmallDeallocations);
				registry.gauge("netty.arena.deallocations.num", tagsWithSize(arenaTags, "normal"), arenaMetric, PoolArenaMetric::numNormalDeallocations);
				registry.gauge("netty.arena.deallocations.num", tagsWithSize(arenaTags, "huge"), arenaMetric, PoolArenaMetric::numHugeDeallocations);
				registry.gauge("netty.arena.allocations.active.num", tagsWithSize(arenaTags, "small"), arenaMetric, PoolArenaMetric::numActiveSmallAllocations);
				registry.gauge("netty.arena.allocations.active.num", tagsWithSize(arenaTags, "normal"), arenaMetric, PoolArenaMetric::numActiveNormalAllocations);
				registry.gauge("netty.arena.allocations.active.num", tagsWithSize(arenaTags, "huge"), arenaMetric, PoolArenaMetric::numActiveHugeAllocations);
				registry.gauge("netty.arena.bytes.active.num", arenaTags, arenaMetric, PoolArenaMetric::numActiveBytes);

				arenaId++;
			}
			registry.gauge("netty.num.thread.local.caches", tags, metric, BufferAllocatorMetric::numThreadLocalCaches);
			registry.gauge("netty.cache.size", tagsWithSize(tags, "small"), metric, BufferAllocatorMetric::smallCacheSize);
			registry.gauge("netty.cache.size", tagsWithSize(tags, "normal"), metric, BufferAllocatorMetric::normalCacheSize);
			registry.gauge("netty.chunk.size", tags, metric, BufferAllocatorMetric::chunkSize);
			registry.gauge("netty.used.memory", tags, metric, BufferAllocatorMetric::usedMemory);
		}
	}

	private List<Tag> tagsWithSize(List<Tag> inputTags, String size) {
		var tags = new ArrayList<>(inputTags);
		tags.add(Tag.of("data.size", size));
		return tags;
	}

	private void registerPoolChunkMetrics(MeterRegistry registry,
			ArrayList<Tag> arenaTags,
			List<PoolChunkListMetric> chunkMetricList) {
		int chunkId = 0;
		for (var chunkMetrics : chunkMetricList) {
			var currentChunkId = chunkId;
			var chunkTags = new ArrayList<>(arenaTags);
			chunkTags.add(Tag.of("chunk.offset", String.valueOf(currentChunkId)));
			registry.gauge("netty.chunk.usage.min", chunkTags, chunkMetrics, PoolChunkListMetric::minUsage);
			registry.gauge("netty.chunk.usage.max", chunkTags, chunkMetrics, PoolChunkListMetric::maxUsage);

			chunkId++;
		}
	}

	public void registerSubpageMetrics(MeterRegistry registry,
			List<Tag> arenaTags,
			List<PoolSubpageMetric> subpageMetricList,
			String subpageType) {

		int subpageId = 0;
		for (PoolSubpageMetric subpageMetric : subpageMetricList) {
			var currentSubpageId = subpageId;
			var subpageTags = new ArrayList<>(arenaTags);
			subpageTags.add(Tag.of("subpage.offset", String.valueOf(currentSubpageId)));
			subpageTags.add(Tag.of("subpage.type", subpageType));
			registry.gauge("netty.subpage.elements.max.num", subpageTags, subpageMetric, PoolSubpageMetric::maxNumElements);
			registry.gauge("netty.subpage.available.num", subpageTags, subpageMetric, PoolSubpageMetric::numAvailable);
			registry.gauge("netty.subpage.elements.size", subpageTags, subpageMetric, PoolSubpageMetric::elementSize);
			registry.gauge("netty.subpage.page.size", subpageTags, subpageMetric, PoolSubpageMetric::pageSize);

			subpageId++;
		}
	}

}
