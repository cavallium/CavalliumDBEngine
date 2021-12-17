package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.lucene.collector.TotalHitCountCollectorManager.TimeLimitingTotalHitCountCollector;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TotalHitCountCollector;

public class TotalHitCountCollectorManager implements CollectorManager<TimeLimitingTotalHitCountCollector, Long> {

	private final Duration timeout;

	public TotalHitCountCollectorManager(Duration timeout) {
		this.timeout = timeout;
	}

	@Override
	public TimeLimitingTotalHitCountCollector newCollector() {
		var totalHitCountCollector = new TotalHitCountCollector();
		var timeLimitingCollector = LuceneUtils.withTimeout(totalHitCountCollector, timeout);
		return new TimeLimitingTotalHitCountCollector(totalHitCountCollector, timeLimitingCollector);
	}

	@Override
	public Long reduce(Collection<TimeLimitingTotalHitCountCollector> collectors) throws IOException {
		long totalHits = 0;
		for (var collector : collectors) {
			totalHits += collector.getTotalHits();
		}
		return totalHits;
	}

	public static final class TimeLimitingTotalHitCountCollector implements Collector {

		private final TotalHitCountCollector totalHitCountCollector;
		private final Collector timeLimitingCollector;

		private TimeLimitingTotalHitCountCollector(TotalHitCountCollector totalHitCountCollector,
				Collector timeLimitingCollector) {
			this.totalHitCountCollector = totalHitCountCollector;
			this.timeLimitingCollector = timeLimitingCollector;
		}

		@Override
		public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
			return timeLimitingCollector.getLeafCollector(context);
		}

		@Override
		public ScoreMode scoreMode() {
			return timeLimitingCollector.scoreMode();
		}

		public long getTotalHits() {
			return totalHitCountCollector.getTotalHits();
		}
	}
}
