package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.serialization.Serializer;
import java.util.Optional;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import static io.netty.buffer.Unpooled.*;

public class DatabaseSingle<U> implements DatabaseStageEntry<U> {

	private final LLDictionary dictionary;
	private final ByteBuf key;
	private final Serializer<U, ByteBuf> serializer;

	public DatabaseSingle(LLDictionary dictionary, ByteBuf key, Serializer<U, ByteBuf> serializer) {
		try {
			this.dictionary = dictionary;
			this.key = key.retain();
			this.serializer = serializer;
		} finally {
			key.release();
		}
	}

	private LLSnapshot resolveSnapshot(@Nullable CompositeSnapshot snapshot) {
		if (snapshot == null) {
			return null;
		} else {
			return snapshot.getSnapshot(dictionary);
		}
	}

	@Override
	public Mono<U> get(@Nullable CompositeSnapshot snapshot, boolean existsAlmostCertainly) {
		return dictionary.get(resolveSnapshot(snapshot), key.retain(), existsAlmostCertainly).map(this::deserialize);
	}

	@Override
	public Mono<U> setAndGetPrevious(U value) {
		ByteBuf valueByteBuf = serialize(value);
		return dictionary
				.put(key.retain(), valueByteBuf.retain(), LLDictionaryResultType.PREVIOUS_VALUE)
				.map(this::deserialize)
				.doFinally(s -> valueByteBuf.release());
	}

	@Override
	public Mono<Boolean> update(Function<@Nullable U, @Nullable U> updater, boolean existsAlmostCertainly) {
		return dictionary.update(key.retain(), (oldValueSer) -> {
			var result = updater.apply(oldValueSer == null ? null : this.deserialize(oldValueSer));
			if (result == null) {
				return null;
			} else {
				return this.serialize(result);
			}
		}, existsAlmostCertainly);
	}

	@Override
	public Mono<U> clearAndGetPrevious() {
		return dictionary.remove(key.retain(), LLDictionaryResultType.PREVIOUS_VALUE).map(this::deserialize);
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot), LLRange.single(key.retain()))
				.map(empty -> empty ? 0L : 1L);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return dictionary
				.isRangeEmpty(resolveSnapshot(snapshot), LLRange.single(key.retain()));
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private U deserialize(ByteBuf bytes) {
		return serializer.deserialize(bytes);
	}

	//todo: temporary wrapper. convert the whole class to buffers
	private ByteBuf serialize(U bytes) {
		return serializer.serialize(bytes);
	}

	@Override
	public void release() {
		key.release();
	}
}