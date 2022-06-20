package it.cavallium.dbengine.database.collections;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class DatabaseLong implements LLKeyValueDatabaseStructure {

	private final LLSingleton singleton;
	private final SerializerFixedBinaryLength<Long> serializer;
	private final SerializerFixedBinaryLength<Integer> bugSerializer;

	public DatabaseLong(LLSingleton singleton) {
		this.singleton = singleton;
		this.serializer = SerializerFixedBinaryLength.longSerializer(singleton.getAllocator());
		this.bugSerializer = SerializerFixedBinaryLength.intSerializer(singleton.getAllocator());
	}

	public Mono<Long> get(@Nullable LLSnapshot snapshot) {
		var resultMono = singleton.get(snapshot);
		return Mono.usingWhen(resultMono,
				result -> Mono.fromSupplier(() -> {
					if (result.readableBytes() == 4) {
						return (long) (int) bugSerializer.deserialize(result);
					} else {
						return serializer.deserialize(result);
					}
				}),
				LLUtils::finalizeResource
		);
	}

	public Mono<Long> incrementAndGet() {
		return addAnd(1, UpdateReturnMode.GET_NEW_VALUE);
	}

	public Mono<Long> getAndIncrement() {
		return addAnd(1, UpdateReturnMode.GET_OLD_VALUE);
	}

	public Mono<Long> decrementAndGet() {
		return addAnd(-1, UpdateReturnMode.GET_NEW_VALUE);
	}

	public Mono<Long> getAndDecrement() {
		return addAnd(-1, UpdateReturnMode.GET_OLD_VALUE);
	}

	public Mono<Long> addAndGet(long count) {
		return addAnd(count, UpdateReturnMode.GET_NEW_VALUE);
	}

	public Mono<Long> getAndAdd(long count) {
		return addAnd(count, UpdateReturnMode.GET_OLD_VALUE);
	}

	private Mono<Long> addAnd(long count, UpdateReturnMode updateReturnMode) {
		var resultMono = singleton.update(prev -> {
			try (prev) {
				if (prev != null) {
					var prevLong = prev.readLong();
					var alloc = singleton.getAllocator();
					var buf = alloc.allocate(Long.BYTES);
					buf.writeLong(prevLong + count);
					return buf;
				} else {
					var alloc = singleton.getAllocator();
					var buf = alloc.allocate(Long.BYTES);
					buf.writeLong(count);
					return buf;
				}
			}
		}, updateReturnMode);
		return Mono.usingWhen(resultMono,
				result -> Mono.fromSupplier(result::readLong),
				LLUtils::finalizeResource
		).single();
	}

	public Mono<Void> set(long value) {
		return singleton.set(Mono.fromCallable(() -> {
			var buf = singleton.getAllocator().allocate(Long.BYTES);
			try {
				serializer.serialize(value, buf);
			} catch (Throwable ex) {
				buf.close();
				throw ex;
			}
			return buf;
		}));
	}

	@Override
	public String getDatabaseName() {
		return singleton.getDatabaseName();
	}
}
