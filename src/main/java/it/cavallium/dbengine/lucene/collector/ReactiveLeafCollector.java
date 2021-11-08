package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.database.LLUtils;
import java.util.concurrent.CancellationException;
import java.util.concurrent.locks.LockSupport;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;

public class ReactiveLeafCollector implements LeafCollector {

	private final LeafReaderContext leafReaderContext;
	private final FluxSink<ScoreDoc> scoreDocsSink;
	private final int shardIndex;

	public ReactiveLeafCollector(LeafReaderContext leafReaderContext, FluxSink<ScoreDoc> scoreDocsSink, int shardIndex) {
		this.leafReaderContext = leafReaderContext;
		this.scoreDocsSink = scoreDocsSink;
		this.shardIndex = shardIndex;
	}

	@Override
	public void setScorer(Scorable scorable) {

	}

	@Override
	public void collect(int i) {
		LLUtils.ensureBlocking();
		var scoreDoc = new ScoreDoc(leafReaderContext.docBase + i, 0, shardIndex);
		while (scoreDocsSink.requestedFromDownstream() < 0 && !scoreDocsSink.isCancelled()) {
			// 100ms
			LockSupport.parkNanos(100L * 1000000L);
		}
		scoreDocsSink.next(scoreDoc);
		if (scoreDocsSink.isCancelled()) {
			throw new CancellationException("Cancelled");
		}
	}
}
