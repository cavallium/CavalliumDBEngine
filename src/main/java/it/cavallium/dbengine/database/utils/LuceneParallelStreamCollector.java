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

	private final LuceneParallelStreamConsumer streamConsumer;
	private final AtomicBoolean stopped;
	private final AtomicLong totalHitsCounter;
	private final ReentrantLock lock;
	private final int base;

	public LuceneParallelStreamCollector(int base, LuceneParallelStreamConsumer streamConsumer,
			AtomicBoolean stopped, AtomicLong totalHitsCounter, ReentrantLock lock) {
		this.base = base;
		this.streamConsumer = streamConsumer;
		this.stopped = stopped;
		this.totalHitsCounter = totalHitsCounter;
		this.lock = lock;
	}

	@Override
	public final LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
		return new LuceneParallelStreamCollector(context.docBase, streamConsumer, stopped, totalHitsCounter, lock);
	}

	@Override
	public void setScorer(Scorable scorer) throws IOException {

	}

	@Override
	public void collect(int doc) throws IOException {
		doc += base;
		totalHitsCounter.incrementAndGet();
		lock.lock();
		try {
			if (!stopped.get()) {
				if (!streamConsumer.consume(doc)) {
					stopped.set(true);
				}
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public ScoreMode scoreMode() {
		return ScoreMode.COMPLETE_NO_SCORES;
	}
}
