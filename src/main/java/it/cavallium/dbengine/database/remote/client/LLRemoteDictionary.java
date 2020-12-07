package it.cavallium.dbengine.database.remote.client;

import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.proto.CavalliumDBEngineServiceGrpc;
import it.cavallium.dbengine.proto.DictionaryMethodClearRequest;
import it.cavallium.dbengine.proto.DictionaryMethodContainsRequest;
import it.cavallium.dbengine.proto.DictionaryMethodForEachRequest;
import it.cavallium.dbengine.proto.DictionaryMethodGetRequest;
import it.cavallium.dbengine.proto.DictionaryMethodIsEmptyRequest;
import it.cavallium.dbengine.proto.DictionaryMethodPutMultiRequest;
import it.cavallium.dbengine.proto.DictionaryMethodPutRequest;
import it.cavallium.dbengine.proto.DictionaryMethodRemoveOneRequest;
import it.cavallium.dbengine.proto.DictionaryMethodRemoveRequest;
import it.cavallium.dbengine.proto.DictionaryMethodReplaceAllRequest;
import it.cavallium.dbengine.proto.DictionaryMethodSizeRequest;
import java.io.IOError;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.concurrency.atomicity.NotAtomic;

@NotAtomic
public class LLRemoteDictionary implements LLDictionary {

	private final CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceBlockingStub blockingStub;
	private final CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceStub stub;
	private final int handle;
	private final String name;

	public LLRemoteDictionary(DbClientFunctions clientFunctions, int handle, String name) {
		this.blockingStub = clientFunctions.getBlockingStub();
		this.stub = clientFunctions.getStub();
		this.handle = handle;
		this.name = name;
	}

	@Override
	public Optional<byte[]> get(@Nullable LLSnapshot snapshot, byte[] key) throws IOException {
		try {
			var request = DictionaryMethodGetRequest.newBuilder()
					.setDictionaryHandle(handle)
					.setKey(ByteString.copyFrom(key));
			if (snapshot != null) {
				request.setSequenceNumber(snapshot.getSequenceNumber());
			}
			var response = blockingStub.dictionaryMethodGet(request.build());
			var value = response.getValue();
			if (value != null) {
				return Optional.of(value.toByteArray());
			} else {
				return Optional.empty();
			}
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public boolean contains(@Nullable LLSnapshot snapshot, byte[] key) throws IOException {
		try {
			var request = DictionaryMethodContainsRequest.newBuilder()
				.setDictionaryHandle(handle)
				.setKey(ByteString.copyFrom(key));
			if (snapshot != null) {
				request.setSequenceNumber(snapshot.getSequenceNumber());
			}
			var response = blockingStub.dictionaryMethodContains(request.build());
			return response.getValue();
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public Optional<byte[]> put(byte[] key, byte[] value, LLDictionaryResultType resultType)
			throws IOException {
		try {
			return put_(key, value, resultType);
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	private Optional<byte[]> put_(byte[] key, byte[] value, LLDictionaryResultType resultType) {
		var response = blockingStub.dictionaryMethodPut(DictionaryMethodPutRequest.newBuilder()
				.setDictionaryHandle(handle)
				.setKey(ByteString.copyFrom(key))
				.setValue(ByteString.copyFrom(value))
				.setResultType(resultType.toProto())
				.build());
		var bytes = response.getValue();
		if (bytes != null) {
			return Optional.of(bytes.toByteArray());
		} else {
			return Optional.empty();
		}
	}

	@Override
	public void putMulti(byte[][] key, byte[][] value, LLDictionaryResultType resultType,
			Consumer<byte[]> responses) throws IOException {
		try {
			var response = blockingStub
					.dictionaryMethodPutMulti(DictionaryMethodPutMultiRequest.newBuilder()
							.setDictionaryHandle(handle)
							.addAllKey(
									List.of(key).stream().map(ByteString::copyFrom).collect(Collectors.toList()))
							.addAllValue(
									List.of(value).stream().map(ByteString::copyFrom).collect(Collectors.toList()))
							.setResultType(resultType.toProto())
							.build());
			if (response.getValueList() != null) {
				for (ByteString byteString : response.getValueList()) {
					responses.accept(byteString.toByteArray());
				}
			}
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public Optional<byte[]> remove(byte[] key, LLDictionaryResultType resultType) throws IOException {
		try {
			return remove_(key, resultType);
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	private Optional<byte[]> remove_(byte[] key, LLDictionaryResultType resultType) {
		var response = blockingStub.dictionaryMethodRemove(DictionaryMethodRemoveRequest.newBuilder()
				.setDictionaryHandle(handle)
				.setKey(ByteString.copyFrom(key))
				.setResultType(resultType.toProto())
				.build());
		var bytes = response.getValue();
		if (bytes != null) {
			return Optional.of(bytes.toByteArray());
		} else {
			return Optional.empty();
		}
	}

	@Override
	public void forEach(@Nullable LLSnapshot snapshot, int parallelism, BiConsumer<byte[], byte[]> consumer) {
		try {
			var request = DictionaryMethodForEachRequest.newBuilder().setDictionaryHandle(handle);
			if (snapshot != null) {
				request.setSequenceNumber(snapshot.getSequenceNumber());
			}
			var response = blockingStub.dictionaryMethodForEach(request.build());
			response.forEachRemaining((entry) -> {
				var key = entry.getKey().toByteArray();
				var value = entry.getValue().toByteArray();
				consumer.accept(key, value);
			});
		} catch (StatusRuntimeException ex) {
			throw new IOError(ex);
		}
	}

	@Override
	public void replaceAll(int parallelism, boolean replaceKeys, BiFunction<byte[], byte[], Entry<byte[], byte[]>> consumer) throws IOException {
		try {
			var response = blockingStub
					.dictionaryMethodReplaceAll(DictionaryMethodReplaceAllRequest.newBuilder()
							.setDictionaryHandle(handle)
							.setReplaceKeys(replaceKeys)
							.build());
			response.forEachRemaining((entry) -> {
				var key = entry.getKey().toByteArray();
				var value = entry.getValue().toByteArray();
				var singleResponse = consumer.apply(key, value);
				boolean keyDiffers = false;
				if (!Arrays.equals(key, singleResponse.getKey())) {
					remove_(key, LLDictionaryResultType.VOID);
					keyDiffers = true;
				}

				// put if changed
				if (keyDiffers || !Arrays.equals(value, singleResponse.getValue())) {
					put_(singleResponse.getKey(), singleResponse.getValue(), LLDictionaryResultType.VOID);
				}
			});
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void clear() throws IOException {
		try {
			//noinspection ResultOfMethodCallIgnored
			blockingStub.dictionaryMethodClear(DictionaryMethodClearRequest.newBuilder()
					.setDictionaryHandle(handle)
					.build());
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public long size(@Nullable LLSnapshot snapshot, boolean fast) throws IOException {
		try {
			var request = DictionaryMethodSizeRequest.newBuilder().setDictionaryHandle(handle);
			if (snapshot != null) {
				request.setSequenceNumber(snapshot.getSequenceNumber());
			}
			var response = fast ? blockingStub.dictionaryMethodFastSize(request.build())
					: blockingStub.dictionaryMethodExactSize(request.build());
			return response.getSize();
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public boolean isEmpty(@Nullable LLSnapshot snapshot) throws IOException {
		try {
			var request = DictionaryMethodIsEmptyRequest
				.newBuilder()
				.setDictionaryHandle(handle);
			if (snapshot != null) {
				request.setSequenceNumber(snapshot.getSequenceNumber());
			}
			var response = blockingStub.dictionaryMethodIsEmpty(request.build());
			return response.getEmpty();
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public Optional<Entry<byte[], byte[]>> removeOne() throws IOException {
		try {
			var response = blockingStub.dictionaryMethodRemoveOne(DictionaryMethodRemoveOneRequest
					.newBuilder()
					.setDictionaryHandle(handle)
					.build());
			var keyBytes = response.getKey();
			var valueBytes = response.getValue();
			if (keyBytes != null && valueBytes != null) {
				return Optional.of(Map.entry(keyBytes.toByteArray(), valueBytes.toByteArray()));
			} else {
				return Optional.empty();
			}
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public String getDatabaseName() {
		return name;
	}
}
