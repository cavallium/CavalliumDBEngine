package it.cavallium.dbengine.lucene;

import it.cavallium.dbengine.lucene.searcher.LocalSearcher;
import it.cavallium.dbengine.lucene.searcher.MultiSearcher;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record LuceneHacks(@Nullable Supplier<@NotNull LocalSearcher> customLocalSearcher,
													@Nullable Supplier<@NotNull MultiSearcher> customMultiSearcher) {}
