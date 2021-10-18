package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;

public class LLLocalKeyPrefixReactiveRocksIterator {

	protected static final Logger logger = LoggerFactory.getLogger(LLLocalKeyPrefixReactiveRocksIterator.class);
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
		return Flux.using(
				() -> range.copy().send(),
				rangeSend -> Flux
						.generate(() -> {
							var readOptions = new ReadOptions(this.readOptions);
							if (!range.hasMin() || !range.hasMax()) {
								readOptions.setReadaheadSize(32 * 1024); // 32KiB
								readOptions.setFillCache(canFillCache);
							}
							if (logger.isTraceEnabled()) {
								logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(range));
							}
							return LLLocalDictionary.getRocksIterator(alloc, allowNettyDirect, readOptions, rangeSend, db, cfh);
						}, (tuple, sink) -> {
							try {
								var rocksIterator = tuple.getT1();
								rocksIterator.status();
								Buffer firstGroupKey = null;
								try {
									while (rocksIterator.isValid()) {
										Buffer key;
										if (allowNettyDirect) {
											key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
										} else {
											key = LLUtils.fromByteArray(alloc, rocksIterator.key());
										}
										try (key) {
											if (firstGroupKey == null) {
												firstGroupKey = key.copy();
											} else if (!LLUtils.equals(firstGroupKey, firstGroupKey.readerOffset(), key, key.readerOffset(),
													prefixLength)) {
												break;
											}
											rocksIterator.next();
											rocksIterator.status();
										}
									}

									if (firstGroupKey != null) {
										assert firstGroupKey.isAccessible();
										var groupKeyPrefix = firstGroupKey.copy(firstGroupKey.readerOffset(), prefixLength);
										assert groupKeyPrefix.isAccessible();

										if (logger.isTraceEnabled()) {
											logger.trace(MARKER_ROCKSDB,
													"Range {} is reading prefix {}",
													LLUtils.toStringSafe(range),
													LLUtils.toStringSafe(groupKeyPrefix)
											);
										}

										sink.next(groupKeyPrefix.send());
									} else {
										if (logger.isTraceEnabled()) {
											logger.trace(MARKER_ROCKSDB, "Range {} ended", LLUtils.toStringSafe(range));
										}
										sink.complete();
									}
								} finally {
									if (firstGroupKey != null) {
										firstGroupKey.close();
									}
								}
							} catch (RocksDBException ex) {
								if (logger.isTraceEnabled()) {
									logger.trace(MARKER_ROCKSDB, "Range {} failed", LLUtils.toStringSafe(range));
								}
								sink.error(ex);
							}
							return tuple;
						}, tuple -> {
							var rocksIterator = tuple.getT1();
							rocksIterator.close();
							tuple.getT2().close();
							tuple.getT3().close();
							tuple.getT4().close();
						}),
				resource -> resource.close()
		);
	}

	public void release() {
		range.close();
	}
}
