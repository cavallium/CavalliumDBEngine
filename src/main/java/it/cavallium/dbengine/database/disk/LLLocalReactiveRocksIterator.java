package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.disk.LLLocalDictionary.getRocksIterator;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
import io.netty.util.IllegalReferenceCountException;
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
	private final BufferAllocator alloc;
	private final ColumnFamilyHandle cfh;
	private final LLRange range;
	private final boolean allowNettyDirect;
	private final ReadOptions readOptions;
	private final boolean readValues;
	private final String debugName;

	public LLLocalReactiveRocksIterator(RocksDB db,
			BufferAllocator alloc,
			ColumnFamilyHandle cfh,
			Send<LLRange> range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean readValues,
			String debugName) {
		this.db = db;
		this.alloc = alloc;
		this.cfh = cfh;
		this.range = range.receive();
		this.allowNettyDirect = allowNettyDirect;
		this.readOptions = readOptions;
		this.readValues = readValues;
		this.debugName = debugName;
	}

	public Flux<T> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					if (!range.hasMin() || !range.hasMax()) {
						readOptions.setReadaheadSize(32 * 1024); // 32KiB
						readOptions.setFillCache(false);
					}
					return getRocksIterator(allowNettyDirect, readOptions, range.copy().send(), db, cfh);
				}, (tuple, sink) -> {
					try {
						var rocksIterator = tuple.getT1();
						rocksIterator.status();
						if (rocksIterator.isValid()) {
							try (Buffer key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key)) {
								Buffer value;
								if (readValues) {
									value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value);
								} else {
									value = alloc.allocate(0);
								}
								try {
									rocksIterator.next();
									rocksIterator.status();
									sink.next(getEntry(key.send(), value.send()));
								} finally {
									value.close();
								}
							}
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
				});
	}

	public abstract T getEntry(Send<Buffer> key, Send<Buffer> value);

	public void release() {
		if (released.compareAndSet(false, true)) {
			range.close();
		} else {
			throw new IllegalReferenceCountException(0, -1);
		}
	}
}
