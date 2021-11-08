package it.cavallium.dbengine.lucene.collector;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Sinks.Many;

public class ReactiveCollectorMultiManager implements CollectorMultiManager<Void, Void> {

	private final FluxSink<ScoreDoc> scoreDocsSink;

	public ReactiveCollectorMultiManager(FluxSink<ScoreDoc> scoreDocsSink) {
		this.scoreDocsSink = scoreDocsSink;
	}

	public CollectorManager<Collector, Void> get(int shardIndex) {
		return new CollectorManager<>() {

			@Override
			public Collector newCollector() {
				return new Collector() {

					@Override
					public LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) {
						return new ReactiveLeafCollector(leafReaderContext, scoreDocsSink, shardIndex);
					}

					@Override
					public ScoreMode scoreMode() {
						return ReactiveCollectorMultiManager.this.scoreMode();
					}
				};
			}

			@Override
			public Void reduce(Collection<Collector> collection) {
				return null;
			}
		};
	}

	@Override
	public ScoreMode scoreMode() {
		return ScoreMode.COMPLETE_NO_SCORES;
	}

	@Override
	public Void reduce(List<Void> results) {
		return null;
	}
}
