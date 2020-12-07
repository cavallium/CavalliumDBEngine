package it.cavallium.dbengine.database.remote.client;

import io.grpc.StatusRuntimeException;
import it.cavallium.dbengine.database.LLDocument;
import it.cavallium.dbengine.database.LLKeyScore;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLSort;
import it.cavallium.dbengine.database.LLTerm;
import it.cavallium.dbengine.database.LLTopKeys;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.proto.CavalliumDBEngineServiceGrpc;
import it.cavallium.dbengine.proto.LuceneIndexCloseRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodAddDocumentMultiRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodAddDocumentRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodCountRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodDeleteAllRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodDeleteDocumentRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodMoreLikeThisRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodSearchRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodSearchResponse;
import it.cavallium.dbengine.proto.LuceneIndexMethodSearchStreamRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodUpdateDocumentMultiRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodUpdateDocumentRequest;
import it.cavallium.dbengine.proto.LuceneIndexSnapshotReleaseRequest;
import it.cavallium.dbengine.proto.LuceneIndexSnapshotTakeRequest;
import it.cavallium.dbengine.proto.MltField;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.batch.ParallelUtils;
import org.warp.commonutils.functional.IOConsumer;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class LLRemoteLuceneIndex implements LLLuceneIndex {

	private final CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceBlockingStub blockingStub;
	private final String luceneIndexName;
	private final int handle;
	private final boolean lowMemory;
	private final int instancesCount;

	public LLRemoteLuceneIndex(DbClientFunctions clientFunctions,
			String name,
			int handle,
			boolean lowMemory,
			int instancesCount) {
		this.blockingStub = clientFunctions.getBlockingStub();
		this.luceneIndexName = name;
		this.handle = handle;
		this.lowMemory = lowMemory;
		this.instancesCount = instancesCount;
	}

	@Override
	public String getLuceneIndexName() {
		return luceneIndexName;
	}

	@Override
	public LLSnapshot takeSnapshot() throws IOException {
		try {
			var searchResult = blockingStub
					.luceneIndexSnapshotTake(LuceneIndexSnapshotTakeRequest.newBuilder()
							.setHandle(handle).build());

			return new LLSnapshot(searchResult.getSequenceNumber());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void releaseSnapshot(LLSnapshot snapshot) throws IOException {
		try {
			//noinspection ResultOfMethodCallIgnored
			blockingStub.luceneIndexSnapshotRelease(LuceneIndexSnapshotReleaseRequest.newBuilder()
					.setHandle(handle)
					.setSequenceNumber(snapshot.getSequenceNumber())
					.build());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void addDocument(LLTerm key, LLDocument doc) throws IOException {
		try {
			//noinspection ResultOfMethodCallIgnored
			blockingStub.luceneIndexMethodAddDocument(LuceneIndexMethodAddDocumentRequest.newBuilder()
					.setHandle(handle)
					.setKey(LLUtils.toGrpc(key))
					.addAllDocumentItems(LLUtils.toGrpc(doc.getItems()))
					.build());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void addDocuments(Iterable<LLTerm> keys, Iterable<LLDocument> docs) throws IOException {
		try {
			//noinspection ResultOfMethodCallIgnored
			blockingStub
					.luceneIndexMethodAddDocumentMulti(LuceneIndexMethodAddDocumentMultiRequest.newBuilder()
							.setHandle(handle)
							.addAllKey(LLUtils.toGrpcKey(keys))
							.addAllDocuments(LLUtils.toGrpc(docs))
							.build());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void deleteDocument(LLTerm id) throws IOException {
		try {
			//noinspection ResultOfMethodCallIgnored
			blockingStub
					.luceneIndexMethodDeleteDocument(LuceneIndexMethodDeleteDocumentRequest.newBuilder()
							.setHandle(handle)
							.setKey(LLUtils.toGrpc(id))
							.build());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void updateDocument(LLTerm id, LLDocument document) throws IOException {
		try {
			//noinspection ResultOfMethodCallIgnored
			blockingStub
					.luceneIndexMethodUpdateDocument(LuceneIndexMethodUpdateDocumentRequest.newBuilder()
							.setHandle(handle)
							.setKey(LLUtils.toGrpc(id))
							.addAllDocumentItems(LLUtils.toGrpc(document.getItems()))
							.build());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void updateDocuments(Iterable<LLTerm> ids, Iterable<LLDocument> documents)
			throws IOException {
		try {
			//noinspection ResultOfMethodCallIgnored
			blockingStub.luceneIndexMethodUpdateDocumentMulti(
					LuceneIndexMethodUpdateDocumentMultiRequest.newBuilder()
							.setHandle(handle)
							.addAllKey(LLUtils.toGrpcKey(ids))
							.addAllDocuments(LLUtils.toGrpc(documents))
							.build());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void deleteAll() throws IOException {
		try {
			//noinspection ResultOfMethodCallIgnored
			blockingStub.luceneIndexMethodDeleteAll(LuceneIndexMethodDeleteAllRequest.newBuilder()
					.setHandle(handle)
					.build());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public Collection<LLTopKeys> search(@Nullable LLSnapshot snapshot,
			String query,
			int limit,
			@Nullable LLSort sort,
			String keyFieldName) throws IOException {
		try {
			ConcurrentLinkedQueue<LLTopKeys> multiResult = new ConcurrentLinkedQueue<>();

			ParallelUtils.parallelizeIO((IOConsumer<Integer> c) -> {
				for (int shardIndex = 0; shardIndex < instancesCount; shardIndex++) {
					c.consume(shardIndex);
				}
			}, 0, instancesCount, 1, shardIndex -> {
				var request = LuceneIndexMethodSearchRequest.newBuilder()
						.setHandle(handle)
						.setQuery(query)
						.setLimit(limit)
						.setKeyFieldName(keyFieldName);
				if (snapshot != null) {
					request.setSequenceNumber(snapshot.getSequenceNumber());
				}
				if (sort != null) {
					request.setSort(LLUtils.toGrpc(sort));
				}

				var searchMultiResults = blockingStub.luceneIndexMethodSearch(request.build());

				for (LuceneIndexMethodSearchResponse response : searchMultiResults.getResponseList()) {
					var result = new LLTopKeys(response.getTotalHitsCount(),
							response.getHitsList().stream().map(LLUtils::toKeyScore).toArray(LLKeyScore[]::new)
					);
					multiResult.add(result);
				}
			});

			return multiResult;
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public Collection<LLTopKeys> moreLikeThis(@Nullable LLSnapshot snapshot, Map<String, Set<String>> mltDocumentFields,
			int limit,
			String keyFieldName) throws IOException {
		try {
			ConcurrentLinkedQueue<LLTopKeys> multiResult = new ConcurrentLinkedQueue<>();

			ParallelUtils.parallelizeIO((IOConsumer<Integer> c) -> {
				for (int shardIndex = 0; shardIndex < instancesCount; shardIndex++) {
					c.consume(shardIndex);
				}
			}, 0, instancesCount, 1, shardIndex -> {
				var request = LuceneIndexMethodMoreLikeThisRequest.newBuilder()
						.setHandle(handle)
						.addAllMltFields(mltDocumentFields
								.entrySet()
								.stream()
								.map(entry -> MltField.newBuilder().setKey(entry.getKey()).addAllValues(entry.getValue()).build())
								.collect(Collectors.toList()))
						.setLimit(limit)
						.setKeyFieldName(keyFieldName);
				if (snapshot != null) {
					request.setSequenceNumber(snapshot.getSequenceNumber());
				}

				var searchMultiResult = blockingStub.luceneIndexMethodMoreLikeThis(request.build());

				for (LuceneIndexMethodSearchResponse response : searchMultiResult.getResponseList()) {
					var result = new LLTopKeys(response.getTotalHitsCount(),
							response.getHitsList().stream().map(LLUtils::toKeyScore).toArray(LLKeyScore[]::new)
					);
					multiResult.add(result);
				}
			});

			return multiResult;
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public Tuple2<Mono<Long>, Collection<Flux<String>>> searchStream(@Nullable LLSnapshot snapshot, String query, int limit, @Nullable LLSort sort, String keyFieldName) {
		try {
			var request = LuceneIndexMethodSearchStreamRequest.newBuilder()
					.setHandle(handle)
					.setQuery(query)
					.setLimit(limit)
					.setKeyFieldName(keyFieldName);
			if (snapshot != null) {
				request.setSequenceNumber(snapshot.getSequenceNumber());
			}
			if (sort != null) {
				request.setSort(LLUtils.toGrpc(sort));
			}

			var searchResult = blockingStub.luceneIndexMethodSearchStream(request.build());

			EmitterProcessor<Long> approximatedTotalHitsCount = EmitterProcessor.create();
			ArrayList<EmitterProcessor<String>> results = new ArrayList<>();
			for (int shardIndex = 0; shardIndex < instancesCount; shardIndex++) {
				results.add(EmitterProcessor.create());
			}
			searchResult.forEachRemaining((result) -> {
				if (result.getIsKey()) {
					results.get(result.getShardIndex()).onNext(result.getKey());
				} else {
					approximatedTotalHitsCount.onNext(result.getApproximatedTotalCount());
				}
			});

			return Tuples.of(approximatedTotalHitsCount.single(0L),
					results.stream().map(EmitterProcessor::asFlux).collect(Collectors.toList())
			);
		} catch (RuntimeException ex) {
			var error = new IOException(ex);
			return Tuples.of(Mono.error(error), Collections.singleton(Flux.error(error)));
		}
	}

	@Override
	public long count(@Nullable LLSnapshot snapshot, String query) throws IOException {
		try {
			var request = LuceneIndexMethodCountRequest.newBuilder()
					.setHandle(handle)
					.setQuery(query);
			if (snapshot != null) {
				request.setSequenceNumber(snapshot.getSequenceNumber());
			}

			var searchResult = blockingStub
					.luceneIndexMethodCount(request.build());

			return searchResult.getCount();
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void close() throws IOException {
		try {
			//noinspection ResultOfMethodCallIgnored
			blockingStub.luceneIndexClose(LuceneIndexCloseRequest.newBuilder()
					.setHandle(handle)
					.build());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public boolean isLowMemoryMode() {
		return lowMemory;
	}
}
