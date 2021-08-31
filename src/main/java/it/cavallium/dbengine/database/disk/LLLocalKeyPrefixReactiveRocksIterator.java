package it.cavallium.dbengine.database.disk;

import io.netty.buffer.api.Buffer;
import io.netty.buffer.api.BufferAllocator;
import io.netty.buffer.api.Send;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import java.util.Arrays;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksMutableObject;
import reactor.core.publisher.Flux;
import static io.netty.buffer.Unpooled.*;

public class LLLocalKeyPrefixReactiveRocksIterator {

	private final RocksDB db;
	private final BufferAllocator alloc;
	private final ColumnFamilyHandle cfh;
	private final int prefixLength;
	private final LLRange range;
	private final boolean allowNettyDirect;
	private final ReadOptions readOptions;
	private final boolean canFillCache;
	private final String debugName;

	public LLLocalKeyPrefixReactiveRocksIterator(RocksDB db, BufferAllocator alloc, ColumnFamilyHandle cfh,
			int prefixLength,
			Send<LLRange> range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean canFillCache,
			String debugName) {
		try (range) {
			this.db = db;
			this.alloc = alloc;
			this.cfh = cfh;
			this.prefixLength = prefixLength;
			this.range = range.receive();
			this.allowNettyDirect = allowNettyDirect;
			this.readOptions = readOptions;
			this.canFillCache = canFillCache;
			this.debugName = debugName;
		}
	}


	public Flux<Send<Buffer>> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					if (!range.hasMin() || !range.hasMax()) {
						readOptions.setReadaheadSize(32 * 1024); // 32KiB
						readOptions.setFillCache(canFillCache);
					}
					return LLLocalDictionary.getRocksIterator(alloc, allowNettyDirect, readOptions, range.copy().send(), db, cfh);
				}, (tuple, sink) -> {
					try {
						var rocksIterator = tuple.getT1();
						rocksIterator.status();
						Buffer firstGroupKey = null;
						try {
							while (rocksIterator.isValid()) {
								try (Buffer key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key).receive()) {
									if (firstGroupKey == null) {
										firstGroupKey = key.copy();
									} else if (!LLUtils.equals(firstGroupKey, firstGroupKey.readerOffset(), key, key.readerOffset(), prefixLength)) {
										break;
									}
									rocksIterator.next();
									rocksIterator.status();
								}
							}
							if (firstGroupKey != null) {
								var groupKeyPrefix = firstGroupKey.copy(firstGroupKey.readerOffset(), prefixLength);
								sink.next(groupKeyPrefix.send());
							} else {
								sink.complete();
							}
						} finally {
							if (firstGroupKey != null) {
								firstGroupKey.close();
							}
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

	public void release() {
		range.close();
	}
}
