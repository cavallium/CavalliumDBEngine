package it.cavallium.dbengine.database.lucene;

import it.cavallium.dbengine.lucene.searcher.LuceneLocalSearcher;
import it.cavallium.dbengine.lucene.searcher.LuceneMultiSearcher;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record LuceneHacks(@Nullable Supplier<@NotNull LuceneLocalSearcher> customLocalSearcher,
													@Nullable Supplier<@NotNull LuceneMultiSearcher> customMultiSearcher) {}
