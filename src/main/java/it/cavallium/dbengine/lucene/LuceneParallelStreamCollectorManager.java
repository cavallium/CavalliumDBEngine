package it.cavallium.dbengine.lucene;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreMode;

public class LuceneParallelStreamCollectorManager implements
		CollectorManager<LuceneParallelStreamCollector, LuceneParallelStreamCollectorResult> {

	private final ScoreMode scoreMode;
	private final LuceneParallelStreamConsumer streamConsumer;
	private final AtomicBoolean stopped;
	private final AtomicLong totalHitsCounter;

	public static LuceneParallelStreamCollectorManager fromConsumer(
			ScoreMode scoreMode,
			LuceneParallelStreamConsumer streamConsumer) {
		return new LuceneParallelStreamCollectorManager(scoreMode, streamConsumer);
	}

	public LuceneParallelStreamCollectorManager(ScoreMode scoreMode,
			LuceneParallelStreamConsumer streamConsumer) {
		this.scoreMode = scoreMode;
		this.streamConsumer = streamConsumer;
		this.stopped = new AtomicBoolean();
		this.totalHitsCounter = new AtomicLong();
	}

	@Override
	public LuceneParallelStreamCollector newCollector() {
		return new LuceneParallelStreamCollector(0, scoreMode, streamConsumer, stopped, totalHitsCounter);
	}

	@Override
	public LuceneParallelStreamCollectorResult reduce(
			Collection<LuceneParallelStreamCollector> collectors) {
		return new LuceneParallelStreamCollectorResult(totalHitsCounter.get());
	}


}
