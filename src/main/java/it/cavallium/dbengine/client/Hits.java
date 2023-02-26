package it.cavallium.dbengine.client;

import com.google.common.collect.Lists;
import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import it.cavallium.dbengine.database.DiscardingCloseable;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SafeCloseable;
import it.cavallium.dbengine.database.collections.ValueGetter;
import it.cavallium.dbengine.lucene.LuceneCloseable;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Hits<T> {

	private static final Logger LOG = LogManager.getLogger(Hits.class);
	private static final Hits<?> EMPTY_HITS = new Hits<>(List.of(), TotalHitsCount.of(0, true));
	private final List<T> results;
	private final TotalHitsCount totalHitsCount;

	public Hits(List<T> results, TotalHitsCount totalHitsCount) {
		this.results = results;
		this.totalHitsCount = totalHitsCount;
	}

	@SuppressWarnings("unchecked")
	public static <T> Hits<T> empty() {
		return (Hits<T>) EMPTY_HITS;
	}

	public static <T, U> Function<Hits<HitKey<T>>, Hits<HitEntry<T, U>>> generateMapper(
			ValueGetter<T, U> valueGetter) {
		return result -> {
			List<HitEntry<T, U>> hitsToTransform = LLUtils.mapList(result.results,
					hit -> new HitEntry<>(hit.key(), valueGetter.get(hit.key()), hit.score())
			);
			return new Hits<>(hitsToTransform, result.totalHitsCount());
		};
	}

	public List<T> results() {
		return results;
	}

	public TotalHitsCount totalHitsCount() {
		return totalHitsCount;
	}

	@Override
	public String toString() {
		return "Hits[" + "results=" + results + ", " + "totalHitsCount=" + totalHitsCount + ']';
	}
}
