package it.cavallium.dbengine.database.collections;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.cavallium.dbengine.database.LLKeyValueDatabaseStructure;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.UpdateReturnMode;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

public class DatabaseLong implements LLKeyValueDatabaseStructure {

	private final LLSingleton singleton;

	public DatabaseLong(LLSingleton singleton) {
		this.singleton = singleton;
	}

	public Mono<Long> get(@Nullable LLSnapshot snapshot) {
		return singleton.get(snapshot).map(array -> {
			if (array.length == 4) {
				return (long) Ints.fromByteArray(array);
			} else {
				return Longs.fromByteArray(array);
			}
		});
	}

	public Mono<Long> incrementAndGet() {
		return incrementAnd(UpdateReturnMode.GET_NEW_VALUE);
	}

	public Mono<Long> getAndIncrement() {
		return incrementAnd(UpdateReturnMode.GET_OLD_VALUE);
	}

	private Mono<Long> incrementAnd(UpdateReturnMode updateReturnMode) {
		return singleton.update(prev -> {
			if (prev != null) {
				try (var prevBuf = prev.receive()) {
					var prevLong = prevBuf.readLong();
					var alloc = singleton.getAllocator();
					var buf = alloc.allocate(Long.BYTES);
					buf.writeLong(prevLong + 1);
					return buf;
				}
			} else {
				var alloc = singleton.getAllocator();
				var buf = alloc.allocate(Long.BYTES);
				buf.writeLong(1);
				return buf;
			}
		}, updateReturnMode).map(send -> {
			try (var buf = send.receive()) {
				return buf.readLong();
			}
		}).single();
	}

	public Mono<Void> set(long value) {
		return singleton.set(Longs.toByteArray(value));
	}

	@Override
	public String getDatabaseName() {
		return singleton.getDatabaseName();
	}
}
