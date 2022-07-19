package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.generateCustomReadOptions;
import static it.cavallium.dbengine.database.LLUtils.isBoundedRange;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.util.Send;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

public class LLLocalKeyPrefixReactiveRocksIterator {

	protected static final Logger logger = LogManager.getLogger(LLLocalKeyPrefixReactiveRocksIterator.class);

	private final RocksDBColumn db;
	private final int prefixLength;
	private final Mono<LLRange> rangeMono;
	private final boolean allowNettyDirect;
	private final Supplier<ReadOptions> readOptions;
	private final boolean canFillCache;
	private final boolean smallRange;

	public LLLocalKeyPrefixReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			Mono<LLRange> rangeMono,
			boolean allowNettyDirect,
			Supplier<ReadOptions> readOptions,
			boolean canFillCache,
			boolean smallRange) {
		this.db = db;
		this.prefixLength = prefixLength;
		this.rangeMono = rangeMono;
		this.allowNettyDirect = allowNettyDirect;
		this.readOptions = readOptions != null ? readOptions : ReadOptions::new;
		this.canFillCache = canFillCache;
		this.smallRange = smallRange;
	}


	public Flux<Buffer> flux() {
		return Flux.usingWhen(rangeMono, range -> Flux.generate(() -> {
			var readOptions
					= generateCustomReadOptions(this.readOptions.get(), canFillCache, isBoundedRange(range), smallRange);
			if (logger.isTraceEnabled()) {
				logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(range));
			}
			return new RocksIterWithReadOpts(readOptions, db.newRocksIterator(allowNettyDirect, readOptions, range, false));
		}, (tuple, sink) -> {
			try {
				var rocksIterator = tuple.iter();
				Buffer firstGroupKey = null;
				try {
					while (rocksIterator.isValid()) {
						Buffer key;
						if (allowNettyDirect) {
							key = LLUtils.readDirectNioBuffer(db.getAllocator(), buffer -> rocksIterator.key(buffer));
						} else {
							key = LLUtils.fromByteArray(db.getAllocator(), rocksIterator.key());
						}
						try (key) {
							var keyLen = key.readableBytes();
							if (keyLen >= prefixLength) {
								if (firstGroupKey == null) {
									firstGroupKey = key.copy();
									assert firstGroupKey == null || firstGroupKey.readableBytes() >= prefixLength;
								} else if (!LLUtils.equals(firstGroupKey,
										firstGroupKey.readerOffset(),
										key,
										key.readerOffset(),
										prefixLength
								)) {
									break;
								}
							} else {
								logger.error("Skipped a key with length {}, the expected minimum prefix key length is {}!"
										+ " This key will be dropped", key.readableBytes(), prefixLength);
							}
							rocksIterator.next();
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

						sink.next(groupKeyPrefix);
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
		}, RocksIterWithReadOpts::close), LLUtils::finalizeResource);
	}

}
