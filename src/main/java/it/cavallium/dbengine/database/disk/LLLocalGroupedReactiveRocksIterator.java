package it.cavallium.dbengine.database.disk;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksMutableObject;
import reactor.core.publisher.Flux;
import static io.netty.buffer.Unpooled.*;

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
								try (Buffer key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key).receive()) {
									if (firstGroupKey == null) {
										firstGroupKey = key.copy();
									} else if (!LLUtils.equals(firstGroupKey, firstGroupKey.readerOffset(),
											key, key.readerOffset(), prefixLength)) {
										break;
									}
									Buffer value;
									if (readValues) {
										value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value).receive();
									} else {
										value = alloc.allocate(0);
									}
									try {
										rocksIterator.next();
										rocksIterator.status();
										T entry = getEntry(key.send(), value.send());
										values.add(entry);
									} finally {
										value.close();
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

	public abstract T getEntry(Send<Buffer> key, Send<Buffer> value);

	public void release() {
		range.close();
	}
}
