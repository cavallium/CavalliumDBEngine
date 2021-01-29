package it.cavallium.dbengine.database.utils;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

public class LuceneParallelStreamCollector implements Collector, LeafCollector {

	private final int base;
	private final ScoreMode scoreMode;
	private final LuceneParallelStreamConsumer streamConsumer;
	private final AtomicBoolean stopped;
	private final AtomicLong totalHitsCounter;
	private final ReentrantLock lock;
	private Scorable scorer;

	public LuceneParallelStreamCollector(int base, ScoreMode scoreMode, LuceneParallelStreamConsumer streamConsumer,
			AtomicBoolean stopped, AtomicLong totalHitsCounter, ReentrantLock lock) {
		this.base = base;
		this.scoreMode = scoreMode;
		this.streamConsumer = streamConsumer;
		this.stopped = stopped;
		this.totalHitsCounter = totalHitsCounter;
		this.lock = lock;
	}

	@Override
	public final LeafCollector getLeafCollector(LeafReaderContext context) {
		return new LuceneParallelStreamCollector(context.docBase, scoreMode, streamConsumer, stopped, totalHitsCounter, lock);
	}

	@Override
	public void setScorer(Scorable scorer) {
		this.scorer = scorer;
	}

	@Override
	public void collect(int doc) throws IOException {
		doc += base;
		totalHitsCounter.incrementAndGet();
		lock.lock();
		try {
			if (!stopped.get()) {
				assert (scorer == null) || scorer.docID() == doc;
				if (!streamConsumer.consume(doc, scorer == null ? 0 : scorer.score())) {
					stopped.set(true);
				}
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public ScoreMode scoreMode() {
		return scoreMode;
	}
}
