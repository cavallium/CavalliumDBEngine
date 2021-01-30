package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.CompositeDatabasePartLocation.CompositeDatabasePartType;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSnapshot;
import java.util.Map;
import java.util.Objects;

public class CompositeSnapshot {
	private final Map<CompositeDatabasePartLocation, LLSnapshot> snapshots;

	public CompositeSnapshot(Map<CompositeDatabasePartLocation, LLSnapshot> snapshots) {
		this.snapshots = snapshots;
	}

	public LLSnapshot getSnapshot(LLKeyValueDatabaseStructure database) {
		return Objects.requireNonNull(snapshots.get(CompositeDatabasePartLocation.of(CompositeDatabasePartType.KV_DATABASE,
				database.getDatabaseName()
		)), () -> "No snapshot for database with name \"" + database.getDatabaseName() + "\"");
	}

	public LLSnapshot getSnapshot(LLLuceneIndex luceneIndex) {
		return Objects.requireNonNull(snapshots.get(CompositeDatabasePartLocation.of(CompositeDatabasePartType.LUCENE_INDEX,
				luceneIndex.getLuceneIndexName()
		)), () -> "No snapshot for lucene index with name \"" + luceneIndex.getLuceneIndexName() + "\"");
	}

	public Map<CompositeDatabasePartLocation, LLSnapshot> getAllSnapshots() {
		return snapshots;
	}
}
