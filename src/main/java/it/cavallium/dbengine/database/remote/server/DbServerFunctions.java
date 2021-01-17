package it.cavallium.dbengine.database.remote.server;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLTopKeys;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.proto.CavalliumDBEngineServiceGrpc;
import it.cavallium.dbengine.proto.DatabaseCloseRequest;
import it.cavallium.dbengine.proto.DatabaseOpenRequest;
import it.cavallium.dbengine.proto.DatabaseSnapshotReleaseRequest;
import it.cavallium.dbengine.proto.DatabaseSnapshotTakeRequest;
import it.cavallium.dbengine.proto.DatabaseSnapshotTakeResult;
import it.cavallium.dbengine.proto.DictionaryMethodClearRequest;
import it.cavallium.dbengine.proto.DictionaryMethodContainsRequest;
import it.cavallium.dbengine.proto.DictionaryMethodContainsResponse;
import it.cavallium.dbengine.proto.DictionaryMethodForEachRequest;
import it.cavallium.dbengine.proto.DictionaryMethodGetRequest;
import it.cavallium.dbengine.proto.DictionaryMethodGetResponse;
import it.cavallium.dbengine.proto.DictionaryMethodIsEmptyRequest;
import it.cavallium.dbengine.proto.DictionaryMethodIsEmptyResponse;
import it.cavallium.dbengine.proto.DictionaryMethodMultiStandardResult;
import it.cavallium.dbengine.proto.DictionaryMethodPutMultiRequest;
import it.cavallium.dbengine.proto.DictionaryMethodPutRequest;
import it.cavallium.dbengine.proto.DictionaryMethodRemoveOneRequest;
import it.cavallium.dbengine.proto.DictionaryMethodRemoveRequest;
import it.cavallium.dbengine.proto.DictionaryMethodSizeRequest;
import it.cavallium.dbengine.proto.DictionaryMethodSizeResponse;
import it.cavallium.dbengine.proto.DictionaryMethodStandardEntityResponse;
import it.cavallium.dbengine.proto.DictionaryMethodStandardResult;
import it.cavallium.dbengine.proto.DictionaryOpenRequest;
import it.cavallium.dbengine.proto.Empty;
import it.cavallium.dbengine.proto.HandleResult;
import it.cavallium.dbengine.proto.LLDocument;
import it.cavallium.dbengine.proto.LLTerm;
import it.cavallium.dbengine.proto.LuceneIndexCloseRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodAddDocumentMultiRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodAddDocumentRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodCountRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodCountResponse;
import it.cavallium.dbengine.proto.LuceneIndexMethodDeleteAllRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodDeleteDocumentRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodMoreLikeThisRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodSearchMultiResponse;
import it.cavallium.dbengine.proto.LuceneIndexMethodSearchRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodSearchResponse;
import it.cavallium.dbengine.proto.LuceneIndexMethodSearchStreamItem;
import it.cavallium.dbengine.proto.LuceneIndexMethodSearchStreamRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodUpdateDocumentMultiRequest;
import it.cavallium.dbengine.proto.LuceneIndexMethodUpdateDocumentRequest;
import it.cavallium.dbengine.proto.LuceneIndexOpenRequest;
import it.cavallium.dbengine.proto.LuceneIndexSnapshotReleaseRequest;
import it.cavallium.dbengine.proto.LuceneIndexSnapshotTakeRequest;
import it.cavallium.dbengine.proto.LuceneIndexSnapshotTakeResult;
import it.cavallium.dbengine.proto.MltField;
import it.cavallium.dbengine.proto.ResetConnectionRequest;
import it.cavallium.dbengine.proto.SingletonMethodGetRequest;
import it.cavallium.dbengine.proto.SingletonMethodGetResponse;
import it.cavallium.dbengine.proto.SingletonMethodSetRequest;
import it.cavallium.dbengine.proto.SingletonOpenRequest;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.warp.commonutils.functional.ConsumerResult;

public class DbServerFunctions extends CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceImplBase {

	private final AtomicInteger firstFreeDbHandle = new AtomicInteger(0);
	private final AtomicInteger firstFreeLuceneHandle = new AtomicInteger(0);
	private final AtomicInteger firstFreeStructureHandle = new AtomicInteger(0);
	private final ConcurrentHashMap<Integer, LLKeyValueDatabase> databases = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Integer, LLLuceneIndex> luceneIndices = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Integer, Set<Integer>> databasesRelatedHandles = new ConcurrentHashMap<>();

	private final ConcurrentHashMap<Integer, LLSingleton> singletons = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Integer, LLDictionary> dictionaries = new ConcurrentHashMap<>();
	private final LLLocalDatabaseConnection localDatabaseConnection;

	public DbServerFunctions(LLLocalDatabaseConnection localDatabaseConnection) {
		this.localDatabaseConnection = localDatabaseConnection;
	}

	@Override
	public void resetConnection(ResetConnectionRequest request,
			StreamObserver<Empty> responseObserver) {
		System.out.println("Resetting connection...");
		int lastHandle = firstFreeDbHandle.get();
		databases.forEach((handle, db) -> {
			System.out.println("Closing db " + handle);
			try {
				db.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		for (int i = 0; i < lastHandle; i++) {
			var relatedHandles = databasesRelatedHandles.remove(i);
			if (relatedHandles != null) {
				for (Integer relatedHandle : relatedHandles) {
					singletons.remove(relatedHandle);
					dictionaries.remove(relatedHandle);
				}
			}
			databases.remove(i);
		}
		responseObserver.onNext(Empty.newBuilder().build());
		responseObserver.onCompleted();
		System.out.println("Connection reset.");
	}

	@Override
	public void databaseOpen(DatabaseOpenRequest request,
			StreamObserver<HandleResult> responseObserver) {
		var response = HandleResult.newBuilder();

		int handle = firstFreeDbHandle.getAndIncrement();

		System.out.println("Opening db " + handle + ".");

		String dbName = Column.toString(request.getName().toByteArray());
		List<Column> columns = request.getColumnNameList().stream()
				.map((nameBinary) -> Column.special(Column.toString(nameBinary.toByteArray())))
				.collect(Collectors.toList());
		boolean lowMemory = request.getLowMemory();

		try {
			var database = localDatabaseConnection.getDatabase(dbName, columns, lowMemory);
			databases.put(handle, database);
			response.setHandle(handle);
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void databaseClose(DatabaseCloseRequest request, StreamObserver<Empty> responseObserver) {
		try {
			System.out.println("Closing db " + request.getDatabaseHandle() + ".");
			var db = databases.remove(request.getDatabaseHandle());
			db.close();
			responseObserver.onNext(Empty.newBuilder().build());
		} catch (Exception e) {
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void luceneIndexOpen(LuceneIndexOpenRequest request,
			StreamObserver<HandleResult> responseObserver) {
		var response = HandleResult.newBuilder();

		int handle = firstFreeLuceneHandle.getAndIncrement();

		System.out.println("Opening lucene " + handle + ".");

		String name = request.getName();
		TextFieldsAnalyzer textFieldsAnalyzer = TextFieldsAnalyzer.values()[request.getTextFieldsAnalyzer()];
		var queryRefreshDebounceTime = Duration.ofMillis(request.getQueryRefreshDebounceTime());
		var commitDebounceTime = Duration.ofMillis(request.getCommitDebounceTime());
		var lowMemory = request.getLowMemory();
		var instancesCount = request.getInstancesCount();

		try {
			var luceneIndex = localDatabaseConnection.getLuceneIndex(name,
					instancesCount,
					textFieldsAnalyzer,
					queryRefreshDebounceTime,
					commitDebounceTime,
					lowMemory
			);
			luceneIndices.put(handle, luceneIndex);
			response.setHandle(handle);
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void luceneIndexClose(LuceneIndexCloseRequest request,
			StreamObserver<Empty> responseObserver) {
		try {
			System.out.println("Closing lucene " + request.getHandle() + ".");
			var luceneIndex = luceneIndices.remove(request.getHandle());
			luceneIndex.close();
			responseObserver.onNext(Empty.newBuilder().build());
		} catch (Exception e) {
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void luceneIndexSnapshotTake(LuceneIndexSnapshotTakeRequest request, StreamObserver<LuceneIndexSnapshotTakeResult> responseObserver) {
		var response = LuceneIndexSnapshotTakeResult.newBuilder();

		int handle = request.getHandle();

		try {
			var snapshot = luceneIndices.get(handle).takeSnapshot();
			response.setSequenceNumber(snapshot.getSequenceNumber());
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@SuppressWarnings("DuplicatedCode")
	@Override
	public void luceneIndexSnapshotRelease(LuceneIndexSnapshotReleaseRequest request, StreamObserver<Empty> responseObserver) {
		var response = Empty.newBuilder();

		int handle = request.getHandle();
		long sequenceNumber = request.getSequenceNumber();

		try {
			luceneIndices.get(handle).releaseSnapshot(new LLSnapshot(sequenceNumber));
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void singletonOpen(SingletonOpenRequest request,
			StreamObserver<HandleResult> responseObserver) {
		var response = HandleResult.newBuilder();

		int handle = firstFreeStructureHandle.getAndIncrement();

		int dbHandle = request.getDatabaseHandle();
		byte[] singletonListColumnName = request.getSingletonListColumnName().toByteArray();
		byte[] name = request.getName().toByteArray();
		byte[] defaultValue = request.getDefaultValue().toByteArray();

		try {
			var singleton = databases.get(dbHandle)
					.getSingleton(singletonListColumnName, name, defaultValue);
			singletons.put(handle, singleton);
			response.setHandle(handle);
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void dictionaryOpen(DictionaryOpenRequest request,
			StreamObserver<HandleResult> responseObserver) {
		var response = HandleResult.newBuilder();

		int handle = firstFreeStructureHandle.getAndIncrement();

		int dbHandle = request.getDatabaseHandle();
		byte[] columnName = request.getColumnName().toByteArray();

		try {
			var dict = databases.get(dbHandle).getDictionary(columnName);
			dictionaries.put(handle, dict);
			response.setHandle(handle);
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void databaseSnapshotTake(DatabaseSnapshotTakeRequest request, StreamObserver<DatabaseSnapshotTakeResult> responseObserver) {
		var response = DatabaseSnapshotTakeResult.newBuilder();

		int dbHandle = request.getDatabaseHandle();

		try {
			var snapshot = databases.get(dbHandle).takeSnapshot();
			response.setSequenceNumber(snapshot.getSequenceNumber());
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void databaseSnapshotRelease(DatabaseSnapshotReleaseRequest request, StreamObserver<Empty> responseObserver) {
		var response = Empty.newBuilder();

		int dbHandle = request.getDatabaseHandle();
		long sequenceNumber = request.getSequenceNumber();

		try {
			databases.get(dbHandle).releaseSnapshot(new LLSnapshot(sequenceNumber));
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void dictionaryMethodGet(DictionaryMethodGetRequest request,
			StreamObserver<DictionaryMethodGetResponse> responseObserver) {
		var response = DictionaryMethodGetResponse.newBuilder();

		int handle = request.getDictionaryHandle();
		long sequenceNumber = request.getSequenceNumber();
		LLSnapshot snapshot = sequenceNumber == 0 ? null : new LLSnapshot(sequenceNumber);
		byte[] key = request.getKey().toByteArray();

		try {
			var dict = dictionaries.get(handle);
			Optional<byte[]> value = dict.get(snapshot, key);
			value.ifPresent(bytes -> response.setValue(ByteString.copyFrom(bytes)));
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void dictionaryMethodContains(DictionaryMethodContainsRequest request,
			StreamObserver<DictionaryMethodContainsResponse> responseObserver) {
		var response = DictionaryMethodContainsResponse.newBuilder();

		int handle = request.getDictionaryHandle();
		long sequenceNumber = request.getSequenceNumber();
		LLSnapshot snapshot = sequenceNumber == 0 ? null : new LLSnapshot(sequenceNumber);
		byte[] key = request.getKey().toByteArray();

		try {
			var dict = dictionaries.get(handle);
			boolean value = dict.contains(snapshot, key);
			response.setValue(value);
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void dictionaryMethodPut(DictionaryMethodPutRequest request,
			StreamObserver<DictionaryMethodStandardResult> responseObserver) {
		var response = DictionaryMethodStandardResult.newBuilder();

		int handle = request.getDictionaryHandle();
		byte[] key = request.getKey().toByteArray();
		byte[] value = request.getValue().toByteArray();
		var resultType = LLDictionaryResultType
				.valueOf(it.cavallium.dbengine.proto.LLDictionaryResultType.forNumber(request.getResultTypeValue()));

		try {
			var dict = dictionaries.get(handle);
			Optional<byte[]> result = dict.put(key, value, resultType);
			result.ifPresent((data) -> response.setValue(ByteString.copyFrom(data)));
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void dictionaryMethodPutMulti(DictionaryMethodPutMultiRequest request,
			StreamObserver<DictionaryMethodMultiStandardResult> responseObserver) {
		var response = DictionaryMethodMultiStandardResult.newBuilder();

		int handle = request.getDictionaryHandle();
		byte[][] key = request.getKeyList().stream().map(ByteString::toByteArray)
				.toArray(byte[][]::new);
		byte[][] value = request.getValueList().stream().map(ByteString::toByteArray)
				.toArray(byte[][]::new);
		var resultType = LLDictionaryResultType
				.valueOf(it.cavallium.dbengine.proto.LLDictionaryResultType.forNumber(request.getResultTypeValue()));

		try {
			var dict = dictionaries.get(handle);
			List<ByteString> responses = new LinkedList<>();
			dict.putMulti(key, value, resultType, (bytes) -> responses.add(ByteString.copyFrom(bytes)));
			if (!responses.isEmpty()) {
				response.addAllValue(responses);
			}
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void dictionaryMethodRemove(DictionaryMethodRemoveRequest request,
			StreamObserver<DictionaryMethodStandardResult> responseObserver) {
		var response = DictionaryMethodStandardResult.newBuilder();

		int handle = request.getDictionaryHandle();
		byte[] key = request.getKey().toByteArray();
		var resultType = LLDictionaryResultType
				.valueOf(it.cavallium.dbengine.proto.LLDictionaryResultType.forNumber(request.getResultTypeValue()));

		try {
			var dict = dictionaries.get(handle);
			Optional<byte[]> result = dict.remove(key, resultType);
			result.ifPresent((data) -> response.setValue(ByteString.copyFrom(data)));
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void dictionaryMethodClear(DictionaryMethodClearRequest request,
			StreamObserver<Empty> responseObserver) {
		int handle = request.getDictionaryHandle();

		try {
			var dict = dictionaries.get(handle);
			dict.clear();
			responseObserver.onNext(Empty.newBuilder().build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void dictionaryMethodFastSize(DictionaryMethodSizeRequest request,
			StreamObserver<DictionaryMethodSizeResponse> responseObserver) {
		var response = DictionaryMethodSizeResponse.newBuilder();

		int handle = request.getDictionaryHandle();
		long sequenceNumber = request.getSequenceNumber();
		LLSnapshot snapshot = sequenceNumber == 0 ? null : new LLSnapshot(sequenceNumber);

		try {
			var dict = dictionaries.get(handle);
			long result = dict.size(snapshot, true);
			response.setSize(result);
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void dictionaryMethodExactSize(DictionaryMethodSizeRequest request,
			StreamObserver<DictionaryMethodSizeResponse> responseObserver) {
		var response = DictionaryMethodSizeResponse.newBuilder();

		int handle = request.getDictionaryHandle();
		long sequenceNumber = request.getSequenceNumber();
		LLSnapshot snapshot = sequenceNumber == 0 ? null : new LLSnapshot(sequenceNumber);

		try {
			var dict = dictionaries.get(handle);
			long result = dict.size(snapshot, false);
			response.setSize(result);
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void dictionaryMethodIsEmpty(DictionaryMethodIsEmptyRequest request,
			StreamObserver<DictionaryMethodIsEmptyResponse> responseObserver) {
		var response = DictionaryMethodIsEmptyResponse.newBuilder();

		int handle = request.getDictionaryHandle();
		long sequenceNumber = request.getSequenceNumber();
		LLSnapshot snapshot = sequenceNumber == 0 ? null : new LLSnapshot(sequenceNumber);

		try {
			var dict = dictionaries.get(handle);
			boolean result = dict.isEmpty(snapshot);
			response.setEmpty(result);
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void dictionaryMethodRemoveOne(DictionaryMethodRemoveOneRequest request,
			StreamObserver<DictionaryMethodStandardEntityResponse> responseObserver) {
		var response = DictionaryMethodStandardEntityResponse.newBuilder();

		int handle = request.getDictionaryHandle();

		try {
			var dict = dictionaries.get(handle);
			Optional<Entry<byte[], byte[]>> result = dict.removeOne();
			result.ifPresent((data) -> {
				response.setKey(ByteString.copyFrom(data.getKey()));
				response.setValue(ByteString.copyFrom(data.getValue()));
			});
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void dictionaryMethodForEach(DictionaryMethodForEachRequest request,
			StreamObserver<DictionaryMethodStandardEntityResponse> responseObserver) {

		int handle = request.getDictionaryHandle();
		long sequenceNumber = request.getSequenceNumber();
		LLSnapshot snapshot = sequenceNumber == 0 ? null : new LLSnapshot(sequenceNumber);

		var dict = dictionaries.get(handle);
		dict.forEach(snapshot, 1, (key, val) -> {
			var response = DictionaryMethodStandardEntityResponse.newBuilder();
			response.setKey(ByteString.copyFrom(key));
			response.setValue(ByteString.copyFrom(val));
			responseObserver.onNext(response.build());
			return ConsumerResult.result();
		});
		responseObserver.onCompleted();
	}

	@Override
	public void singletonMethodGet(SingletonMethodGetRequest request,
			StreamObserver<SingletonMethodGetResponse> responseObserver) {
		var response = SingletonMethodGetResponse.newBuilder();

		int handle = request.getSingletonHandle();
		long sequenceNumber = request.getSequenceNumber();
		LLSnapshot snapshot = sequenceNumber == 0 ? null : new LLSnapshot(sequenceNumber);

		try {
			var singleton = singletons.get(handle);
			byte[] result = singleton.get(snapshot);
			response.setValue(ByteString.copyFrom(result));
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void singletonMethodSet(SingletonMethodSetRequest request,
			StreamObserver<Empty> responseObserver) {
		int handle = request.getSingletonHandle();
		byte[] value = request.getValue().toByteArray();

		try {
			var singleton = singletons.get(handle);
			singleton.set(value);
			responseObserver.onNext(Empty.newBuilder().build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}


	@Override
	public void luceneIndexMethodAddDocument(LuceneIndexMethodAddDocumentRequest request,
			StreamObserver<Empty> responseObserver) {
		int handle = request.getHandle();
		var documentKey = request.getKey();
		var documentItemsList = request.getDocumentItemsList();

		try {
			var luceneIndex = luceneIndices.get(handle);
			luceneIndex.addDocument(LLUtils.toLocal(documentKey), LLUtils.toLocal(documentItemsList));
			responseObserver.onNext(Empty.newBuilder().build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void luceneIndexMethodAddDocumentMulti(LuceneIndexMethodAddDocumentMultiRequest request,
			StreamObserver<Empty> responseObserver) {
		int handle = request.getHandle();
		List<LLTerm> keyList = request.getKeyList();
		List<LLDocument> documentItemsList = request.getDocumentsList();

		try {
			var luceneIndex = luceneIndices.get(handle);
			luceneIndex.addDocuments(LLUtils.toLocalTerms(keyList), LLUtils.toLocalDocuments(documentItemsList));
			responseObserver.onNext(Empty.newBuilder().build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void luceneIndexMethodDeleteDocument(LuceneIndexMethodDeleteDocumentRequest request,
			StreamObserver<Empty> responseObserver) {
		int handle = request.getHandle();
		var key = request.getKey();

		try {
			var luceneIndex = luceneIndices.get(handle);
			luceneIndex.deleteDocument(LLUtils.toLocal(key));
			responseObserver.onNext(Empty.newBuilder().build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void luceneIndexMethodUpdateDocument(LuceneIndexMethodUpdateDocumentRequest request,
			StreamObserver<Empty> responseObserver) {
		int handle = request.getHandle();
		var key = request.getKey();
		var documentItemsList = request.getDocumentItemsList();

		try {
			var luceneIndex = luceneIndices.get(handle);
			luceneIndex.updateDocument(LLUtils.toLocal(key), LLUtils.toLocal(documentItemsList));
			responseObserver.onNext(Empty.newBuilder().build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void luceneIndexMethodUpdateDocumentMulti(
			LuceneIndexMethodUpdateDocumentMultiRequest request, StreamObserver<Empty> responseObserver) {
		int handle = request.getHandle();
		List<LLTerm> keyList = request.getKeyList();
		List<LLDocument> documentItemsList = request.getDocumentsList();

		try {
			var luceneIndex = luceneIndices.get(handle);
			luceneIndex.updateDocuments(LLUtils.toLocalTerms(keyList),
					LLUtils.toLocalDocuments(documentItemsList));
			responseObserver.onNext(Empty.newBuilder().build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void luceneIndexMethodDeleteAll(LuceneIndexMethodDeleteAllRequest request,
			StreamObserver<Empty> responseObserver) {
		int handle = request.getHandle();

		try {
			var luceneIndex = luceneIndices.get(handle);
			luceneIndex.deleteAll();
			responseObserver.onNext(Empty.newBuilder().build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void luceneIndexMethodSearch(LuceneIndexMethodSearchRequest request,
			StreamObserver<LuceneIndexMethodSearchMultiResponse> responseObserver) {
		int handle = request.getHandle();
		var snapshot = request.getSequenceNumber() == 0 ? null : new LLSnapshot(request.getSequenceNumber());
		var query = request.getQuery();
		var limit = request.getLimit();
		var sort = request.hasSort() ? LLUtils.toLocal(request.getSort()) : null;
		var keyFieldName = request.getKeyFieldName();

		try {
			var luceneIndex = luceneIndices.get(handle);
			var multiResults = luceneIndex.search(snapshot, query, limit, sort, keyFieldName);
			List<LuceneIndexMethodSearchResponse> responses = new ArrayList<>();
			for (LLTopKeys result : multiResults) {
				var response = LuceneIndexMethodSearchResponse.newBuilder()
						.setTotalHitsCount(result.getTotalHitsCount())
						.addAllHits(ObjectArrayList.wrap(result.getHits()).stream().map(LLUtils::toGrpc)
								.collect(Collectors.toList()));
				responses.add(response.build());
			}
			responseObserver.onNext(LuceneIndexMethodSearchMultiResponse.newBuilder().addAllResponse(responses).build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void luceneIndexMethodMoreLikeThis(LuceneIndexMethodMoreLikeThisRequest request,
			StreamObserver<LuceneIndexMethodSearchMultiResponse> responseObserver) {
		int handle = request.getHandle();
		var snapshot = request.getSequenceNumber() == 0 ? null : new LLSnapshot(request.getSequenceNumber());
		var mltFieldsList = request.getMltFieldsList();
		var limit = request.getLimit();
		var keyFieldName = request.getKeyFieldName();

		try {
			var luceneIndex = luceneIndices.get(handle);

			var mltFields = new HashMap<String, Set<String>>();
			for (MltField mltField : mltFieldsList) {
				mltFields.put(mltField.getKey(), new HashSet<>(mltField.getValuesList()));
			}

			var multiResults = luceneIndex.moreLikeThis(snapshot, mltFields, limit, keyFieldName);
			List<LuceneIndexMethodSearchResponse> responses = new ArrayList<>();
			for (LLTopKeys result : multiResults) {
				var response = LuceneIndexMethodSearchResponse
						.newBuilder()
						.setTotalHitsCount(result.getTotalHitsCount())
						.addAllHits(ObjectArrayList.wrap(result.getHits()).stream().map(LLUtils::toGrpc).collect(Collectors.toList()));
				responses.add(response.build());
			}
			responseObserver.onNext(LuceneIndexMethodSearchMultiResponse.newBuilder().addAllResponse(responses).build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void luceneIndexMethodSearchStream(LuceneIndexMethodSearchStreamRequest request,
			StreamObserver<LuceneIndexMethodSearchStreamItem> responseObserver) {
		int handle = request.getHandle();
		var snapshot = request.getSequenceNumber() == 0 ? null : new LLSnapshot(request.getSequenceNumber());
		var query = request.getQuery();
		var limit = request.getLimit();
		var sort = request.hasSort() ? LLUtils.toLocal(request.getSort()) : null;
		var keyFieldName = request.getKeyFieldName();

		try {
			var luceneIndex = luceneIndices.get(handle);
			var results = luceneIndex.searchStream(snapshot, query, limit, sort, keyFieldName);
			int shardIndex = 0;
			for (var flux : results.getT2()) {
				int shardIndexF = shardIndex;
				flux.subscribe(resultKey -> responseObserver.onNext(LuceneIndexMethodSearchStreamItem
						.newBuilder()
						.setShardIndex(shardIndexF)
						.setIsKey(true)
						.setKey(resultKey)
						.build()), responseObserver::onError, responseObserver::onCompleted);

				shardIndex++;
			}
			results
					.getT1()
					.subscribe(count -> responseObserver.onNext(LuceneIndexMethodSearchStreamItem
							.newBuilder()
							.setIsKey(false)
							.setApproximatedTotalCount(count)
							.build()), responseObserver::onError, responseObserver::onCompleted);
		} catch (Exception e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
	}

	@Override
	public void luceneIndexMethodCount(LuceneIndexMethodCountRequest request,
			StreamObserver<LuceneIndexMethodCountResponse> responseObserver) {
		int handle = request.getHandle();
		var snapshot = request.getSequenceNumber() == 0 ? null : new LLSnapshot(request.getSequenceNumber());
		var query = request.getQuery();

		try {
			var luceneIndex = luceneIndices.get(handle);
			var result = luceneIndex.count(snapshot, query);
			var response = LuceneIndexMethodCountResponse.newBuilder()
					.setCount(result);
			responseObserver.onNext(response.build());
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void ping(Empty request, StreamObserver<Empty> responseObserver) {
		responseObserver.onNext(Empty.newBuilder().build());
		responseObserver.onCompleted();
	}
}
