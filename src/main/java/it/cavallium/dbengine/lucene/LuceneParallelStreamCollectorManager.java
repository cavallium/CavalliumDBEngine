package it.cavallium.dbengine.lucene;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreMode;
import org.jetbrains.annotations.Nullable;

public class LuceneParallelStreamCollectorManager implements
		CollectorManager<LuceneParallelStreamCollector, LuceneParallelStreamCollectorResult> {

	private final ScoreMode scoreMode;
	@Nullable
	private final Float minCompetitiveScore;
	private final LuceneParallelStreamConsumer streamConsumer;
	private final AtomicBoolean stopped;
	private final AtomicLong totalHitsCounter;

	public static LuceneParallelStreamCollectorManager fromConsumer(
			ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			LuceneParallelStreamConsumer streamConsumer) {
		return new LuceneParallelStreamCollectorManager(scoreMode, minCompetitiveScore, streamConsumer);
	}

	public LuceneParallelStreamCollectorManager(ScoreMode scoreMode,
			@Nullable Float minCompetitiveScore,
			LuceneParallelStreamConsumer streamConsumer) {
		this.scoreMode = scoreMode;
		this.minCompetitiveScore = minCompetitiveScore;
		this.streamConsumer = streamConsumer;
		this.stopped = new AtomicBoolean();
		this.totalHitsCounter = new AtomicLong();
	}

	@Override
	public LuceneParallelStreamCollector newCollector() {
		return new LuceneParallelStreamCollector(0,
				scoreMode,
				minCompetitiveScore,
				streamConsumer,
				stopped,
				totalHitsCounter
		);
	}

	@Override
	public LuceneParallelStreamCollectorResult reduce(
			Collection<LuceneParallelStreamCollector> collectors) {
		return new LuceneParallelStreamCollectorResult(totalHitsCounter.get());
	}


}
