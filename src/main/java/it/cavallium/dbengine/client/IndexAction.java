package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.IndexAction.Add;
import it.cavallium.dbengine.client.IndexAction.AddMulti;
import it.cavallium.dbengine.client.IndexAction.Update;
import it.cavallium.dbengine.client.IndexAction.UpdateMulti;
import it.cavallium.dbengine.client.IndexAction.Delete;
import it.cavallium.dbengine.client.IndexAction.DeleteAll;
import it.cavallium.dbengine.client.IndexAction.TakeSnapshot;
import it.cavallium.dbengine.client.IndexAction.ReleaseSnapshot;
import it.cavallium.dbengine.client.IndexAction.Flush;
import it.cavallium.dbengine.client.IndexAction.Refresh;
import it.cavallium.dbengine.client.IndexAction.Close;
import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTerm;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.One;

sealed interface IndexAction permits Add, AddMulti, Update, UpdateMulti, Delete, DeleteAll, TakeSnapshot,
		ReleaseSnapshot, Flush, Refresh, Close {

	IndexActionType getType();

	final record Add(LLTerm key, LLDocument doc, MonoSink<Void> addedFuture) implements IndexAction {

		@Override
		public IndexActionType getType() {
			return IndexActionType.ADD;
		}
	}

	final record AddMulti(Flux<Entry<LLTerm, LLDocument>> docsFlux, MonoSink<Void> addedMultiFuture) implements IndexAction {

		@Override
		public IndexActionType getType() {
			return IndexActionType.ADD_MULTI;
		}
	}

	final record Update(LLTerm key, LLDocument doc, MonoSink<Void> updatedFuture) implements IndexAction {

		@Override
		public IndexActionType getType() {
			return IndexActionType.UPDATE;
		}
	}

	final record UpdateMulti(Map<LLTerm, LLDocument> docs, MonoSink<Void> updatedMultiFuture) implements IndexAction {

		@Override
		public IndexActionType getType() {
			return IndexActionType.UPDATE_MULTI;
		}
	}

	final record Delete(LLTerm key, MonoSink<Void> deletedFuture) implements IndexAction {

		@Override
		public IndexActionType getType() {
			return IndexActionType.DELETE;
		}
	}

	final record DeleteAll(MonoSink<Void> deletedAllFuture) implements IndexAction {

		@Override
		public IndexActionType getType() {
			return IndexActionType.DELETE_ALL;
		}
	}

	final record TakeSnapshot(MonoSink<LLSnapshot> snapshotFuture) implements IndexAction {

		@Override
		public IndexActionType getType() {
			return IndexActionType.TAKE_SNAPSHOT;
		}
	}

	final record ReleaseSnapshot(LLSnapshot snapshot, MonoSink<Void> releasedFuture) implements IndexAction {

		@Override
		public IndexActionType getType() {
			return IndexActionType.RELEASE_SNAPSHOT;
		}
	}

	final record Flush(MonoSink<Void> flushFuture) implements IndexAction {

		@Override
		public IndexActionType getType() {
			return IndexActionType.FLUSH;
		}
	}

	final record Refresh(boolean force, MonoSink<Void> refreshFuture) implements IndexAction {

		@Override
		public IndexActionType getType() {
			return IndexActionType.REFRESH;
		}
	}

	final record Close(MonoSink<Void> closeFuture) implements IndexAction {

		@Override
		public IndexActionType getType() {
			return IndexActionType.CLOSE;
		}
	}

	enum IndexActionType {
		ADD,
		ADD_MULTI,
		UPDATE,
		UPDATE_MULTI,
		DELETE,
		DELETE_ALL,
		TAKE_SNAPSHOT,
		RELEASE_SNAPSHOT,
		FLUSH,
		REFRESH,
		CLOSE
	}
}
