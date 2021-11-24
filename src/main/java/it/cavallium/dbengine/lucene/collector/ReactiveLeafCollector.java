package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.database.LLUtils;
import java.util.concurrent.locks.LockSupport;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;

public class ReactiveLeafCollector implements LeafCollector {

	private final LeafReaderContext leafReaderContext;
	private final FluxSink<ScoreDoc> scoreDocsSink;
	private final int shardIndex;
	private final Thread requestThread;

	public ReactiveLeafCollector(LeafReaderContext leafReaderContext,
			FluxSink<ScoreDoc> scoreDocsSink,
			int shardIndex,
			Thread requestThread) {
		this.leafReaderContext = leafReaderContext;
		this.scoreDocsSink = scoreDocsSink;
		this.shardIndex = shardIndex;
		this.requestThread = requestThread;
	}

	@Override
	public void setScorer(Scorable scorable) {

	}

	@Override
	public void collect(int i) {
		// Assert that we are running on the request thread
		assert Thread.currentThread() == requestThread;
		// Assert that this is a non-blocking context
		assert !Schedulers.isInNonBlockingThread();

		// Wait if no requests from downstream are found
		boolean cancelled;
		while (!(cancelled = scoreDocsSink.isCancelled()) && scoreDocsSink.requestedFromDownstream() <= 0) {
			// 1000ms
			LockSupport.parkNanos(1000L * 1000000L);
		}
		// Cancel execution throwing this specific lucene error
		if (cancelled) {
			throw new CollectionTerminatedException();
		}

		// Send the response
		var scoreDoc = new ScoreDoc(leafReaderContext.docBase + i, 0, shardIndex);
		scoreDocsSink.next(scoreDoc);

	}
}
