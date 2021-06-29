package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.disk.LLLocalDictionary.getRocksIterator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksMutableObject;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple3;

import static io.netty.buffer.Unpooled.*;
import static it.cavallium.dbengine.database.disk.LLLocalDictionary.logger;

public abstract class LLLocalReactiveRocksIterator<T> {

	private final AtomicBoolean released = new AtomicBoolean(false);
	private final RocksDB db;
	private final ByteBufAllocator alloc;
	private final ColumnFamilyHandle cfh;
	private final LLRange range;
	private final boolean allowNettyDirect;
	private final ReadOptions readOptions;
	private final boolean readValues;
	private final String debugName;

	public LLLocalReactiveRocksIterator(RocksDB db,
			ByteBufAllocator alloc,
			ColumnFamilyHandle cfh,
			LLRange range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean readValues,
			String debugName) {
		this.db = db;
		this.alloc = alloc;
		this.cfh = cfh;
		this.range = range;
		this.allowNettyDirect = allowNettyDirect;
		this.readOptions = readOptions;
		this.readValues = readValues;
		this.debugName = debugName;
	}

	public Flux<T> flux() {
		return Flux
				.<T, @NotNull Tuple3<RocksIterator, ReleasableSlice, ReleasableSlice>>generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					if (!range.hasMin() || !range.hasMax()) {
						readOptions.setReadaheadSize(32 * 1024); // 32KiB
						readOptions.setFillCache(false);
					}
					return getRocksIterator(allowNettyDirect, readOptions, range.retain(), db, cfh);
				}, (tuple, sink) -> {
					range.retain();
					try {
						var rocksIterator = tuple.getT1();
						rocksIterator.status();
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
									rocksIterator.status();
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
					} catch (RocksDBException ex) {
						sink.error(ex);
					} finally {
						range.release();
					}
					return tuple;
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
		if (released.compareAndSet(false, true)) {
			range.release();
		} else {
			throw new IllegalStateException("Already released");
		}
	}
}
