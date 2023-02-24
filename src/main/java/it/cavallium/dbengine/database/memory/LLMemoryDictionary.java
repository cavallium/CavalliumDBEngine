package it.cavallium.dbengine.database.memory;

import static it.cavallium.dbengine.utils.StreamUtils.count;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.client.BadBlock;
import it.cavallium.dbengine.database.LLDelta;
import it.cavallium.dbengine.database.LLDictionary;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.OptionalBuf;
import it.cavallium.dbengine.database.SerializedKey;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.disk.BinarySerializationFunction;
import it.cavallium.dbengine.database.serialization.KVSerializationFunction;
import it.cavallium.dbengine.database.serialization.SerializationException;
import it.cavallium.dbengine.utils.DBException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

public class LLMemoryDictionary implements LLDictionary {

	private final String databaseName;
	private final String columnName;
	private final UpdateMode updateMode;
	private final Getter<Long, ConcurrentSkipListMap<Buf, Buf>> snapshots;
	private final ConcurrentSkipListMap<Buf, Buf> mainDb;

	private interface Getter<T, U> {
		U get(T argument);
	}

	public LLMemoryDictionary(String databaseName,
			String columnName,
			UpdateMode updateMode,
			ConcurrentHashMap<Long, ConcurrentHashMap<String, ConcurrentSkipListMap<Buf, Buf>>> snapshots,
			ConcurrentHashMap<String, ConcurrentSkipListMap<Buf, Buf>> mainDb) {
		this.databaseName = databaseName;
		this.columnName = columnName;
		this.updateMode = updateMode;
		this.snapshots = (snapshotId) -> snapshots.get(snapshotId).get(columnName);
		this.mainDb = mainDb.get(columnName);
	}

	@Override
	public String getColumnName() {
		return columnName;
	}

	private long resolveSnapshot(LLSnapshot snapshot) {
		if (snapshot == null) {
			return Long.MIN_VALUE + 1L;
		} else if (snapshot.getSequenceNumber() == Long.MIN_VALUE + 1L) {
			throw new IllegalStateException();
		} else {
			return snapshot.getSequenceNumber();
		}
	}

	private Buf transformResult(Buf result, LLDictionaryResultType resultType) {
		if (resultType == LLDictionaryResultType.PREVIOUS_VALUE) {
			// Don't retain the result because it has been removed from the skip list
			return kkB(result);
		} else if (resultType == LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE) {
			return LLUtils.booleanToResponseByteBuffer(result != null);
		} else {
			return null;
		}
	}

	private Buf k(Buf buf) {
		if (buf == null) return null;
		return buf;
	}

	private Buf kShr(Buf buf) {
		if (buf == null) return null;
		return buf;
	}

	private Buf kOwn(Buf buf) {
		if (buf == null) return null;
		return buf;
	}

	private Buf kk(Buf bytesList) {
		if (bytesList == null) return null;
		return bytesList;
	}

	private Buf kkB(Buf bytesList) {
		if (bytesList == null) return null;
		return bytesList;
	}

	private BLRange r(Supplier<LLRange> send) {
		var range = send.get();
		if (range.isAll()) {
			return new BLRange(null, null, null);
		} else if (range.isSingle()) {
			return new BLRange(null, null, k(range.getSingle()));
		} else if (range.hasMin() && range.hasMax()) {
			return new BLRange(k(range.getMin()), k(range.getMax()), null);
		} else if (range.hasMin()) {
			return new BLRange(k(range.getMin()), null, null);
		} else {
			return new BLRange(k(range.getMax()), null, null);
		}
	}

	private ConcurrentNavigableMap<Buf, Buf> mapSlice(LLSnapshot snapshot, LLRange range) {
		if (range.isAll()) {
			return snapshots.get(resolveSnapshot(snapshot));
		} else if (range.isSingle()) {
			var key = k(range.getSingle());
			var value = snapshots
					.get(resolveSnapshot(snapshot))
					.get(key);
			if (value != null) {
				return new ConcurrentSkipListMap<>(Map.of(key, value));
			} else {
				return new ConcurrentSkipListMap<>(Map.of());
			}
		} else if (range.hasMin() && range.hasMax()) {
			var min = k(range.getMin());
			var max = k(range.getMax());
			if (min.compareTo(max) > 0) {
				return new ConcurrentSkipListMap<>(Map.of());
			}
			return snapshots
					.get(resolveSnapshot(snapshot))
					.subMap(min, true, max, false);
		} else if (range.hasMin()) {
			return snapshots
					.get(resolveSnapshot(snapshot))
					.tailMap(k(range.getMin()), true);
		} else {
			return snapshots
					.get(resolveSnapshot(snapshot))
					.headMap(k(range.getMax()), false);
		}
	}

	@Override
	public Buf get(@Nullable LLSnapshot snapshot, Buf key) {
		return snapshots.get(resolveSnapshot(snapshot)).get(kShr(key));
	}

	@Override
	public Buf put(Buf key, Buf value, LLDictionaryResultType resultType) {
		var result = mainDb.put(key, value);
		return this.transformResult(result, resultType);
	}

	@Override
	public UpdateMode getUpdateMode() {
		return updateMode;
	}

	@Override
	public LLDelta updateAndGetDelta(Buf key, BinarySerializationFunction updater) {
		if (updateMode == UpdateMode.DISALLOW) {
			throw new UnsupportedOperationException("update() is disallowed");
		}
		AtomicReference<Buf> oldRef = new AtomicReference<>(null);
		var newValue = mainDb.compute(kShr(key), (_unused, old) -> {
			if (old != null) {
				oldRef.set(old);
			}
			Buf v;
			var oldToSend = old != null ? kkB(old) : null;
			try {
				v = updater.apply(oldToSend);
			} catch (SerializationException e) {
				throw new IllegalStateException(e);
			}
			if (v != null) {
				return kOwn(v);
			} else {
				return null;
			}
		});
		var oldVal = oldRef.get();
		return LLDelta.of(oldVal != null ? kkB(oldRef.get()) : null, newValue != null ? kkB(newValue) : null);
	}

	@Override
	public void clear() {
		mainDb.clear();
	}

	@Override
	public Buf remove(Buf key, LLDictionaryResultType resultType) {
		var prev = mainDb.remove(kShr(key));
		// Don't retain the result because it has been removed from the skip list
		if (prev != null) {
			return switch (resultType) {
				case VOID -> null;
				case PREVIOUS_VALUE_EXISTENCE -> LLUtils.booleanToResponseByteBuffer(true);
				case PREVIOUS_VALUE -> kkB(prev);
			};
		} else {
			if (resultType == LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE) {
				return LLUtils.booleanToResponseByteBuffer(false);
			} else {
				return null;
			}
		}
	}

	@Override
	public Stream<OptionalBuf> getMulti(@Nullable LLSnapshot snapshot, Stream<Buf> keys) {
		return keys.map(key -> {
			Buf v = snapshots.get(resolveSnapshot(snapshot)).get(k(key));
			if (v != null) {
				return OptionalBuf.of(kkB(v));
			} else {
				return OptionalBuf.empty();
			}
		});
	}

	@Override
	public void putMulti(Stream<LLEntry> entries) {
		entries.forEach(entry -> mainDb.put(k(entry.getKey()), k(entry.getValue())));
	}

	@Override
	public <K> Stream<Boolean> updateMulti(Stream<SerializedKey<K>> keys,
			KVSerializationFunction<K, @Nullable Buf, @Nullable Buf> updateFunction) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public Stream<LLEntry> getRange(@Nullable LLSnapshot snapshot,
			LLRange range,
			boolean reverse,
			boolean smallRange) {
		if (range.isSingle()) {
			var single = range.getSingle();
			var element = snapshots.get(resolveSnapshot(snapshot)).get(k(single));
			if (element != null) {
				return Stream.of(LLEntry.of(single, kkB(element)));
			} else {
				return Stream.empty();
			}
		} else {
			var map = mapSlice(snapshot, range);

			Set<Entry<Buf, Buf>> set;
			if (reverse) {
				set = map.descendingMap().entrySet();
			} else {
				set = map.entrySet();
			}
			return set.stream().map(entry -> LLEntry.of(kkB(entry.getKey()), kkB(entry.getValue())));
		}
	}

	@Override
	public Stream<List<LLEntry>> getRangeGrouped(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength, boolean smallRange) {
		if (range.isSingle()) {
			var single = range.getSingle();
			var element = snapshots.get(resolveSnapshot(snapshot)).get(k(single));
			if (element != null) {
				return Stream.of(List.of(LLEntry.of(single, kkB(element))));
			} else {
				return Stream.empty();
			}
		} else {
			return mapSlice(snapshot, range)
					.entrySet()
					.stream()
					.collect(groupingBy(k -> k.getKey().subList(0, prefixLength),
							mapping(entry -> LLEntry.of(kkB(entry.getKey()), kkB(entry.getValue())), Collectors.toList())
					))
					.values()
					.stream();
		}
	}

	@Override
	public Stream<Buf> getRangeKeys(@Nullable LLSnapshot snapshot, LLRange range, boolean reverse, boolean smallRange) {
		if (range.isSingle()) {
			var single = range.getSingle();
			var contains = snapshots.get(resolveSnapshot(snapshot)).containsKey(k(single));
			return contains ? Stream.of(single) : Stream.empty();
		} else {
			var map = mapSlice(snapshot, range);
			if (reverse) {
				map = map.descendingMap();
			}
			return map.keySet().stream().map(this::kkB);
		}
	}

	@Override
	public Stream<List<Buf>> getRangeKeysGrouped(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength, boolean smallRange) {
		if (range.isSingle()) {
			var single = range.getSingle();
			var containsElement = snapshots.get(resolveSnapshot(snapshot)).containsKey(k(single));
			if (containsElement) {
				return Stream.of(List.of(single));
			} else {
				return Stream.empty();
			}
		} else {
			return mapSlice(snapshot, range)
					.entrySet()
					.stream()
					.collect(groupingBy(k -> k.getKey().subList(0, prefixLength),
							mapping(entry -> kkB(entry.getKey()), Collectors.toList())
					))
					.values()
					.stream();
		}
	}

	@Override
	public Stream<Buf> getRangeKeyPrefixes(@Nullable LLSnapshot snapshot,
			LLRange range,
			int prefixLength,
			boolean smallRange) {
		if (range.isSingle()) {
			var single = range.getSingle();
			var k = k(single);
			var containsElement = snapshots.get(resolveSnapshot(snapshot)).containsKey(k);
			if (containsElement) {
				return Stream.of(kkB(k.subList(0, prefixLength)));
			} else {
				return Stream.empty();
			}
		} else {
			return mapSlice(snapshot, range).keySet().stream()
					.map(bytes -> bytes.subList(0, prefixLength))
					.distinct()
					.map(this::kkB);
		}
	}

	@Override
	public Stream<BadBlock> badBlocks(LLRange range) {
		return Stream.empty();
	}

	@Override
	public void setRange(LLRange range, Stream<LLEntry> entries, boolean smallRange) {
		if (range.isSingle()) {
			var single = range.getSingle();
			var k = k(single);
			mainDb.remove(k);
		} else {
			mapSlice(null, range).clear();
		}

		var r = r(range::copy);

		entries.forEach(entry -> {
			if (!isInsideRange(r, kShr(entry.getKey()))) {
				throw new IndexOutOfBoundsException("Trying to set a key outside the range!");
			}
			mainDb.put(kShr(entry.getKey()), kShr(entry.getValue()));
		});
	}

	private boolean isInsideRange(BLRange range, Buf key) {
		if (range.isAll()) {
			return true;
		} else if (range.isSingle()) {
			var single = range.getSingle();
			return Objects.equals(single, key);
		} else if (range.hasMin() && range.hasMax()) {
			var min = range.getMin();
			var max = range.getMax();
			return min.compareTo(key) <= 0 && max.compareTo(key) > 0;
		} else if (range.hasMin()) {
			var min = range.getMin();
			return min.compareTo(key) <= 0;
		} else {
			var max = range.getMax();
			return max.compareTo(key) > 0;
		}
	}

	@Override
	public boolean isRangeEmpty(@Nullable LLSnapshot snapshot, LLRange range, boolean fillCache) {
		return count(getRangeKeys(snapshot, range, false, false)) == 0;
	}

	@Override
	public long sizeRange(@Nullable LLSnapshot snapshot, LLRange range, boolean fast) {
		return mapSlice(snapshot, range).size();
	}

	@Override
	public LLEntry getOne(@Nullable LLSnapshot snapshot, LLRange range) {
		return getRange(snapshot, range, false, false).findAny().orElse(null);
	}

	@Override
	public Buf getOneKey(@Nullable LLSnapshot snapshot, LLRange range) {
		return getRangeKeys(snapshot, range, false, false).findAny().orElse(null);
	}

	@Override
	public LLEntry removeOne(LLRange range) {
		if (range.isSingle()) {
			var single = range.getSingle();
			var element = mainDb.remove(k(single));
			if (element != null) {
				return LLEntry.of(single, kkB(element));
			} else {
				return null;
			}
		} else {
			var map = mapSlice(null, range);
			var it = map.entrySet().iterator();
			if (it.hasNext()) {
				var next = it.next();
				it.remove();
				return LLEntry.of(kkB(next.getKey()), kkB(next.getValue()));
			} else {
				return null;
			}
		}
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}
}
