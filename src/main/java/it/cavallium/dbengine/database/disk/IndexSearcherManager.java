package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.SafeCloseable;
import java.io.IOException;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;

public interface IndexSearcherManager extends SafeCloseable {

	void maybeRefreshBlocking();

	void maybeRefresh();

	LLIndexSearcher retrieveSearcher(@Nullable LLSnapshot snapshot);
}
