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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

public class LLLocalKeyPrefixReactiveRocksIterator extends
		ResourceSupport<LLLocalKeyPrefixReactiveRocksIterator, LLLocalKeyPrefixReactiveRocksIterator> {

	protected static final Logger logger = LogManager.getLogger(LLLocalKeyPrefixReactiveRocksIterator.class);
	private static final Drop<LLLocalKeyPrefixReactiveRocksIterator> DROP = new Drop<>() {
		@Override
		public void drop(LLLocalKeyPrefixReactiveRocksIterator obj) {
			try {
				if (obj.rangeShared != null && obj.rangeShared.isAccessible()) {
					obj.rangeShared.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close range", ex);
			}
			try {
				if (obj.readOptions != null && obj.readOptions.isAccessible()) {
					obj.readOptions.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close readOptions", ex);
			}
		}

		@Override
		public Drop<LLLocalKeyPrefixReactiveRocksIterator> fork() {
			return this;
		}

		@Override
		public void attach(LLLocalKeyPrefixReactiveRocksIterator obj) {

		}
	};

	private final RocksDBColumn db;
	private final int prefixLength;
	private LLRange rangeShared;
	private final boolean allowNettyDirect;
	private ReadOptions readOptions;
	private final boolean canFillCache;
	private final boolean smallRange;

	public LLLocalKeyPrefixReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			LLRange range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean canFillCache,
			boolean smallRange) {
		super(DROP);
		this.db = db;
		this.prefixLength = prefixLength;
		this.rangeShared = range;
		this.allowNettyDirect = allowNettyDirect;
		this.readOptions = readOptions != null ? readOptions : new ReadOptions();
		this.canFillCache = canFillCache;
		this.smallRange = smallRange;
	}


	public Flux<Buffer> flux() {
		return Flux.generate(() -> {
			var readOptions = generateCustomReadOptions(this.readOptions,
					canFillCache,
					isBoundedRange(rangeShared),
					smallRange
			);
			if (logger.isTraceEnabled()) {
				logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(rangeShared));
			}
			return new RocksIterWithReadOpts(readOptions, db.newRocksIterator(allowNettyDirect, readOptions, rangeShared, false));
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
									LLUtils.toStringSafe(rangeShared),
									LLUtils.toStringSafe(groupKeyPrefix)
							);
						}

						sink.next(groupKeyPrefix);
					} else {
						if (logger.isTraceEnabled()) {
							logger.trace(MARKER_ROCKSDB, "Range {} ended", LLUtils.toStringSafe(rangeShared));
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
					logger.trace(MARKER_ROCKSDB, "Range {} failed", LLUtils.toStringSafe(rangeShared));
				}
				sink.error(ex);
			}
			return tuple;
		}, RocksIterWithReadOpts::close);
	}

	@Override
	protected final RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLLocalKeyPrefixReactiveRocksIterator> prepareSend() {
		var range = this.rangeShared.send();
		var readOptions = this.readOptions;
		return drop -> new LLLocalKeyPrefixReactiveRocksIterator(db,
				prefixLength,
				range.receive(),
				allowNettyDirect,
				readOptions,
				canFillCache,
				smallRange
		);
	}

	protected void makeInaccessible() {
		this.rangeShared = null;
		this.readOptions = null;
	}
}
