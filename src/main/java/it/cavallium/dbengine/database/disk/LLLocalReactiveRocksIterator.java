package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.disk.LLLocalDictionary.getRocksIterator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksMutableObject;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple3;

import static io.netty.buffer.Unpooled.*;

public abstract class LLLocalReactiveRocksIterator<T> {

	private final RocksDB db;
	private final ByteBufAllocator alloc;
	private final ColumnFamilyHandle cfh;
	private final LLRange range;
	private final ReadOptions readOptions;
	private final boolean readValues;

	public LLLocalReactiveRocksIterator(RocksDB db,
			ByteBufAllocator alloc,
			ColumnFamilyHandle cfh,
			LLRange range,
			ReadOptions readOptions,
			boolean readValues) {
		this.db = db;
		this.alloc = alloc;
		this.cfh = cfh;
		this.range = range;
		this.readOptions = readOptions;
		this.readValues = readValues;
	}

	public Flux<T> flux() {
		return Flux
				.<T, @NotNull Tuple3<RocksIterator, ReleasableSlice, ReleasableSlice>>generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					if (!range.hasMin() || !range.hasMax()) {
						readOptions.setReadaheadSize(2 * 1024 * 1024);
						readOptions.setFillCache(false);
					}
					return getRocksIterator(readOptions, range.retain(), db, cfh);
				}, (tuple, sink) -> {
					range.retain();
					try {
						var rocksIterator = tuple.getT1();
						if (rocksIterator.isValid()) {
							ByteBuf key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
							try {
								ByteBuf value;
								if (readValues) {
									value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value);
								} else {
									value = alloc.buffer(0);
								}
								try {
									rocksIterator.next();
									sink.next(getEntry(key.retain(), value.retain()));
								} finally {
									value.release();
								}
							} finally {
								key.release();
							}
						} else {
							sink.complete();
						}
						return tuple;
					} finally {
						range.release();
					}
				}, tuple -> {
					var rocksIterator = tuple.getT1();
					rocksIterator.close();
					tuple.getT2().release();
					tuple.getT3().release();
				})
				.doFirst(range::retain)
				.doAfterTerminate(range::release);
	}

	public abstract T getEntry(ByteBuf key, ByteBuf value);

	public void release() {
		range.release();
	}
}
