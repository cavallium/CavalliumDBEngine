package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.disk.LLLocalDictionary.getRocksIterator;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.Send;
import io.netty5.util.IllegalReferenceCountException;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import java.util.concurrent.atomic.AtomicBoolean;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;

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
					return getRocksIterator(alloc, allowNettyDirect, readOptions, range.copy().send(), db, cfh);
				}, (tuple, sink) -> {
					try {
						var rocksIterator = tuple.getT1();
						rocksIterator.status();
						if (rocksIterator.isValid()) {
							Buffer key;
							if (allowNettyDirect) {
								key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key).receive();
							} else {
								key = LLUtils.fromByteArray(alloc, rocksIterator.key());
							}
							try (key) {
								Buffer value;
								if (readValues) {
									if (allowNettyDirect) {
										value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value).receive();
									} else {
										value = LLUtils.fromByteArray(alloc, rocksIterator.value());
									}
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
					tuple.getT4().close();
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
