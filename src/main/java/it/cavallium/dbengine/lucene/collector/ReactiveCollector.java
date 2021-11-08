package it.cavallium.dbengine.lucene.collector;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Sinks.Many;

public class ReactiveCollector implements Collector {

	private final FluxSink<ScoreDoc> scoreDocsSink;
	private int shardIndex;

	public ReactiveCollector(FluxSink<ScoreDoc> scoreDocsSink) {
		this.scoreDocsSink = scoreDocsSink;
	}

	@Override
	public LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) {
		return new ReactiveLeafCollector(leafReaderContext, scoreDocsSink, shardIndex);
	}

	@Override
	public ScoreMode scoreMode() {
		return ScoreMode.COMPLETE_NO_SCORES;
	}

	public void setShardIndex(int shardIndex) {
		this.shardIndex = shardIndex;
	}
}
