package it.cavallium.dbengine.database.utils;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.search.CollectorManager;

public class LuceneParallelStreamCollectorManager implements
		CollectorManager<LuceneParallelStreamCollector, LuceneParallelStreamCollectorResult> {

	private final LuceneParallelStreamConsumer streamConsumer;
	private final AtomicBoolean stopped;
	private final AtomicLong totalHitsCounter;
	private final ReentrantLock lock;

	public static LuceneParallelStreamCollectorManager fromConsumer(
			LuceneParallelStreamConsumer streamConsumer) {
		return new LuceneParallelStreamCollectorManager(streamConsumer);
	}

	public LuceneParallelStreamCollectorManager(LuceneParallelStreamConsumer streamConsumer) {
		this.streamConsumer = streamConsumer;
		this.stopped = new AtomicBoolean();
		this.totalHitsCounter = new AtomicLong();
		this.lock = new ReentrantLock();
	}

	@Override
	public LuceneParallelStreamCollector newCollector() throws IOException {
		return new LuceneParallelStreamCollector(0, streamConsumer, stopped, totalHitsCounter, lock);
	}

	@Override
	public LuceneParallelStreamCollectorResult reduce(
			Collection<LuceneParallelStreamCollector> collectors) throws IOException {
		return new LuceneParallelStreamCollectorResult(totalHitsCounter.get());
	}


}
