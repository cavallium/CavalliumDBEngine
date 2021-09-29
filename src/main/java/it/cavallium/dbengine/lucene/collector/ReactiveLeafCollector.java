package it.cavallium.dbengine.lucene.collector;

import it.cavallium.dbengine.database.LLUtils;
import java.util.concurrent.locks.LockSupport;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;

public class ReactiveLeafCollector implements LeafCollector {

	private final LeafReaderContext leafReaderContext;
	private final Many<ScoreDoc> scoreDocsSink;
	private final int shardIndex;

	public ReactiveLeafCollector(LeafReaderContext leafReaderContext, Many<ScoreDoc> scoreDocsSink, int shardIndex) {
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
		boolean shouldRetry;
		do {
			var currentError = scoreDocsSink.tryEmitNext(scoreDoc);
			shouldRetry = currentError == EmitResult.FAIL_NON_SERIALIZED || currentError == EmitResult.FAIL_OVERFLOW
					|| currentError == EmitResult.FAIL_ZERO_SUBSCRIBER;
			if (shouldRetry) {
				LockSupport.parkNanos(10);
			} else {
				currentError.orThrow();
			}

		} while (shouldRetry);
	}
}
