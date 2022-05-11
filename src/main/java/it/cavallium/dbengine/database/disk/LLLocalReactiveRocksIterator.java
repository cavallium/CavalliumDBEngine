package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.generateCustomReadOptions;
import static it.cavallium.dbengine.database.LLUtils.isBoundedRange;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.Send;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

public abstract class LLLocalReactiveRocksIterator<T> extends
		ResourceSupport<LLLocalReactiveRocksIterator<T>, LLLocalReactiveRocksIterator<T>> {

	protected static final Logger logger = LogManager.getLogger(LLLocalReactiveRocksIterator.class);
	private static final Drop<LLLocalReactiveRocksIterator<?>> DROP = new Drop<>() {
		@Override
		public void drop(LLLocalReactiveRocksIterator<?> obj) {
			try {
				if (obj.rangeShared != null) {
					obj.rangeShared.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close range", ex);
			}
			try {
				if (obj.readOptions != null) {
					obj.readOptions.close();
				}
			} catch (Throwable ex) {
				logger.error("Failed to close readOptions", ex);
			}
		}

		@Override
		public Drop<LLLocalReactiveRocksIterator<?>> fork() {
			return this;
		}

		@Override
		public void attach(LLLocalReactiveRocksIterator<?> obj) {

		}
	};

	private final RocksDBColumn db;
	private LLRange rangeShared;
	private final boolean allowNettyDirect;
	private ReadOptions readOptions;
	private final boolean readValues;
	private final boolean reverse;
	private final boolean smallRange;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public LLLocalReactiveRocksIterator(RocksDBColumn db,
			Send<LLRange> range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean readValues,
			boolean reverse,
			boolean smallRange) {
		super((Drop<LLLocalReactiveRocksIterator<T>>) (Drop) DROP);
		try (range) {
			this.db = db;
			this.rangeShared = range.receive();
			this.allowNettyDirect = allowNettyDirect;
			this.readOptions = readOptions;
			this.readValues = readValues;
			this.reverse = reverse;
			this.smallRange = smallRange;
		}
	}

	public final Flux<T> flux() {
		return Flux.generate(() -> {
			var readOptions = generateCustomReadOptions(this.readOptions, true, isBoundedRange(rangeShared), smallRange);
			if (logger.isTraceEnabled()) {
				logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(rangeShared));
			}
			return new RocksIterWithReadOpts(readOptions, db.newRocksIterator(allowNettyDirect, readOptions, rangeShared, reverse));
		}, (tuple, sink) -> {
			try {
				var rocksIterator = tuple.iter().iterator();
				if (rocksIterator.isValid()) {
					Buffer key;
					if (allowNettyDirect) {
						key = LLUtils.readDirectNioBuffer(db.getAllocator(), rocksIterator::key);
					} else {
						key = LLUtils.fromByteArray(db.getAllocator(), rocksIterator.key());
					}
					try (key) {
						Buffer value;
						if (readValues) {
							if (allowNettyDirect) {
								value = LLUtils.readDirectNioBuffer(db.getAllocator(), rocksIterator::value);
							} else {
								value = LLUtils.fromByteArray(db.getAllocator(), rocksIterator.value());
							}
						} else {
							value = null;
						}

						if (logger.isTraceEnabled()) {
							logger.trace(MARKER_ROCKSDB,
									"Range {} is reading {}: {}",
									LLUtils.toStringSafe(rangeShared),
									LLUtils.toStringSafe(key),
									LLUtils.toStringSafe(value)
							);
						}

						try {
							if (reverse) {
								rocksIterator.prev();
							} else {
								rocksIterator.next();
							}
							sink.next(getEntry(key.send(), value == null ? null : value.send()));
						} finally {
							if (value != null) {
								value.close();
							}
						}
					}
				} else {
					if (logger.isTraceEnabled()) {
						logger.trace(MARKER_ROCKSDB, "Range {} ended", LLUtils.toStringSafe(rangeShared));
					}
					sink.complete();
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

	public abstract T getEntry(@Nullable Send<Buffer> key, @Nullable Send<Buffer> value);

	@Override
	protected final RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLLocalReactiveRocksIterator<T>> prepareSend() {
		var range = this.rangeShared.send();
		var readOptions = this.readOptions;
		return drop -> new LLLocalReactiveRocksIterator<>(db,
				range,
				allowNettyDirect,
				readOptions,
				readValues,
				reverse,
				smallRange
		) {
			@Override
			public T getEntry(@Nullable Send<Buffer> key, @Nullable Send<Buffer> value) {
				return LLLocalReactiveRocksIterator.this.getEntry(key, value);
			}
		};
	}

	protected void makeInaccessible() {
		this.rangeShared = null;
		this.readOptions = null;
	}
}
