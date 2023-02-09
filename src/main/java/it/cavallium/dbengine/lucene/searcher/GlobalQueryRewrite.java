package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.database.disk.LLIndexSearchers;
import java.io.IOException;

public interface GlobalQueryRewrite {

	GlobalQueryRewrite NO_REWRITE = (indexSearchers, queryParamsMono) -> queryParamsMono;

	LocalQueryParams rewrite(LLIndexSearchers indexSearchers, LocalQueryParams localQueryParams);
}
