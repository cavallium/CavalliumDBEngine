package it.cavallium.dbengine.database.collections;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.client.CompositeSnapshot;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.Delta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.serialization.Serializer;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.util.Optional;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
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
		return Mono
				.defer(() -> dictionary.get(resolveSnapshot(snapshot), key.retain(), existsAlmostCertainly))
				.map(this::deserialize)
				.doFirst(key::retain)
				.doAfterTerminate(key::release);
	}

	@Override
	public Mono<U> setAndGetPrevious(U value) {
		return Mono
				.using(
						() -> serialize(value),
						valueByteBuf -> dictionary
								.put(key.retain(), valueByteBuf.retain(), LLDictionaryResultType.PREVIOUS_VALUE)
								.map(this::deserialize),
						ReferenceCounted::release
				)
				.doFirst(key::retain)
				.doAfterTerminate(key::release);
	}

	@Override
	public Mono<U> update(Function<@Nullable U, @Nullable U> updater,
			UpdateReturnMode updateReturnMode,
			boolean existsAlmostCertainly) {
		return Mono
				.defer(() -> dictionary.update(key.retain(), (oldValueSer) -> {
					var result = updater.apply(oldValueSer == null ? null : this.deserialize(oldValueSer));
					if (result == null) {
						return null;
					} else {
						return this.serialize(result);
					}
				}, updateReturnMode, existsAlmostCertainly))
				.map(this::deserialize)
				.doFirst(key::retain)
				.doAfterTerminate(key::release);
	}

	@Override
	public Mono<Delta<U>> updateAndGetDelta(Function<@Nullable U, @Nullable U> updater,
			boolean existsAlmostCertainly) {
		return Mono
				.defer(() -> dictionary.updateAndGetDelta(key.retain(), (oldValueSer) -> {
					var result = updater.apply(oldValueSer == null ? null : this.deserialize(oldValueSer));
					if (result == null) {
						return null;
					} else {
						return this.serialize(result);
					}
				}, existsAlmostCertainly).transform(mono -> LLUtils.mapDelta(mono, this::deserialize)))
				.doFirst(key::retain)
				.doAfterTerminate(key::release);
	}

	@Override
	public Mono<U> clearAndGetPrevious() {
		return Mono
				.defer(() -> dictionary
						.remove(key.retain(), LLDictionaryResultType.PREVIOUS_VALUE)
				)
				.map(this::deserialize)
				.doFirst(key::retain)
				.doAfterTerminate(key::release);
	}

	@Override
	public Mono<Long> leavesCount(@Nullable CompositeSnapshot snapshot, boolean fast) {
		return Mono
				.defer(() -> dictionary
						.isRangeEmpty(resolveSnapshot(snapshot), LLRange.single(key.retain()))
				)
				.map(empty -> empty ? 0L : 1L)
				.doFirst(key::retain)
				.doAfterTerminate(key::release);
	}

	@Override
	public Mono<Boolean> isEmpty(@Nullable CompositeSnapshot snapshot) {
		return Mono
				.defer(() -> dictionary
						.isRangeEmpty(resolveSnapshot(snapshot), LLRange.single(key.retain()))
				)
				.doFirst(key::retain)
				.doAfterTerminate(key::release);
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

	@Override
	public Flux<BadBlock> badBlocks() {
		return this
				.get(null, true)
				.then(Mono.<BadBlock>empty())
				.onErrorResume(ex -> Mono.just(new BadBlock(dictionary.getDatabaseName(),
						Column.special(dictionary.getDatabaseName()),
						ByteList.of(LLUtils.toArray(key)),
						ex
				)))
				.flux();
	}
}