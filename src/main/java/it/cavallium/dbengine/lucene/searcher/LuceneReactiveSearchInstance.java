package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.lucene.searcher.LuceneStreamSearcher.ResultItemConsumer;
import java.io.IOException;
import reactor.core.publisher.Flux;

public record LuceneReactiveSearchInstance(long totalHitsCount, Flux<LLKeyScore> results) {
}
