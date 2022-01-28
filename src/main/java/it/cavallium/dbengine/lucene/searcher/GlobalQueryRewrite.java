package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import java.io.IOException;
import reactor.core.publisher.Mono;

public interface GlobalQueryRewrite {

	GlobalQueryRewrite NO_REWRITE = (indexSearchers, queryParamsMono) -> queryParamsMono;

	LocalQueryParams rewrite(LLIndexSearchers indexSearchers, LocalQueryParams localQueryParams) throws IOException;
}
