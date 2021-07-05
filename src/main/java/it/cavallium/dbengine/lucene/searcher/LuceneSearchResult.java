package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.LLKeyScore;
import java.io.IOException;
import reactor.core.publisher.Flux;

public record LuceneSearchResult(long totalHitsCount, Flux<LLKeyScore> results) {
}
