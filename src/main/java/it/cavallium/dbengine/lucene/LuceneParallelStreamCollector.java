package it.cavallium.dbengine.lucene;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.jetbrains.annotations.Nullable;

public class LuceneParallelStreamCollector implements Collector, LeafCollector {

	private final int base;
	private final ScoreMode scoreMode;
	@Nullable
	private final Float minCompetitiveScore;
	private final LuceneParallelStreamConsumer streamConsumer;
	private final AtomicBoolean stopped;
	private final AtomicLong totalHitsCounter;
	private Scorable scorer;

	public LuceneParallelStreamCollector(int base,
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			LuceneParallelStreamConsumer streamConsumer,
			AtomicBoolean stopped,
			AtomicLong totalHitsCounter) {
		this.base = base;
		this.scoreMode = scoreMode;
		this.minCompetitiveScore = minCompetitiveScore;
		this.streamConsumer = streamConsumer;
		this.stopped = stopped;
		this.totalHitsCounter = totalHitsCounter;
	}

	@Override
	public final LeafCollector getLeafCollector(LeafReaderContext context) {
		return new LuceneParallelStreamCollector(context.docBase,
				scoreMode,
				minCompetitiveScore,
				streamConsumer,
				stopped,
				totalHitsCounter
		);
	}

	@Override
	public void setScorer(Scorable scorer) throws IOException {
		this.scorer = scorer;
		if (minCompetitiveScore != null && !minCompetitiveScore.isNaN() && !minCompetitiveScore.isInfinite()) {
			scorer.setMinCompetitiveScore(minCompetitiveScore);
		}
	}

	@Override
	public void collect(int doc) throws IOException {
		doc += base;
		totalHitsCounter.incrementAndGet();
		if (!stopped.get()) {
			var score = scorer == null ? 0 : scorer.score();
			if (minCompetitiveScore == null || score >= minCompetitiveScore) {
				if (!streamConsumer.consume(doc, score)) {
					stopped.set(true);
				}
			}
		}
	}

	@Override
	public ScoreMode scoreMode() {
		return scoreMode;
	}
}
