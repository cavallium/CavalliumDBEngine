package it.cavallium.dbengine.database.disk;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;

public abstract class LLLocalGroupedReactiveRocksIterator<T> {

	private final RocksDB db;
	private final BufferAllocator alloc;
	private final ColumnFamilyHandle cfh;
	private final int prefixLength;
	private final LLRange range;
	private final boolean allowNettyDirect;
	private final ReadOptions readOptions;
	private final boolean canFillCache;
	private final boolean readValues;

	public LLLocalGroupedReactiveRocksIterator(RocksDB db, BufferAllocator alloc, ColumnFamilyHandle cfh,
			int prefixLength,
			Send<LLRange> range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean canFillCache,
			boolean readValues) {
		try (range) {
			this.db = db;
			this.alloc = alloc;
			this.cfh = cfh;
			this.prefixLength = prefixLength;
			this.range = range.receive();
			this.allowNettyDirect = allowNettyDirect;
			this.readOptions = readOptions;
			this.canFillCache = canFillCache;
			this.readValues = readValues;
		}
	}


	public Flux<List<T>> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					readOptions.setFillCache(canFillCache && range.hasMin() && range.hasMax());
					return LLLocalDictionary.getRocksIterator(alloc, allowNettyDirect, readOptions, range.copy().send(), db, cfh);
				}, (tuple, sink) -> {
					try {
						var rocksIterator = tuple.getT1();
						ObjectArrayList<T> values = new ObjectArrayList<>();
						Buffer firstGroupKey = null;
						try {
							rocksIterator.status();
							while (rocksIterator.isValid()) {
								try (Buffer key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key)) {
									if (firstGroupKey == null) {
										firstGroupKey = key.copy();
									} else if (!LLUtils.equals(firstGroupKey, firstGroupKey.readerOffset(),
											key, key.readerOffset(), prefixLength)) {
										break;
									}
									@Nullable Buffer value;
									if (readValues) {
										value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value);
									} else {
										value = null;
									}
									try {
										rocksIterator.next();
										rocksIterator.status();
										T entry = getEntry(key.send(), value == null ? null : value.send());
										values.add(entry);
									} finally {
										if (value != null) {
											value.close();
										}
									}
								}
							}
						} finally {
							if (firstGroupKey != null) {
								firstGroupKey.close();
							}
						}
						if (!values.isEmpty()) {
							sink.next(values);
						} else {
							sink.complete();
						}
					} catch (RocksDBException ex) {
						sink.error(ex);
					}
					return tuple;
				}, tuple -> {
					var rocksIterator = tuple.getT1();
					rocksIterator.close();
					tuple.getT2().close();
					tuple.getT3().close();
					tuple.getT4().close();
				});
	}

	public abstract T getEntry(@Nullable Send<Buffer> key, @Nullable Send<Buffer> value);

	public void release() {
		range.close();
	}
}
