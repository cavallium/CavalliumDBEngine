package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.disk.LLLocalDictionary.getRocksIterator;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;

public abstract class LLLocalReactiveRocksIterator<T> extends
		ResourceSupport<LLLocalReactiveRocksIterator<T>, LLLocalReactiveRocksIterator<T>> {

	protected static final Logger logger = LoggerFactory.getLogger(LLLocalReactiveRocksIterator.class);
	private static final Drop<LLLocalReactiveRocksIterator<?>> DROP = new Drop<>() {
		@Override
		public void drop(LLLocalReactiveRocksIterator<?> obj) {
			try {
				if (obj.range != null) {
					obj.range.close();
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
	private LLRange range;
	private final boolean allowNettyDirect;
	private ReadOptions readOptions;
	private final boolean readValues;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public LLLocalReactiveRocksIterator(RocksDBColumn db,
			Send<LLRange> range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean readValues) {
		super((Drop<LLLocalReactiveRocksIterator<T>>) (Drop) DROP);
		this.db = db;
		this.range = range.receive();
		this.allowNettyDirect = allowNettyDirect;
		this.readOptions = readOptions;
		this.readValues = readValues;
	}

	public final Flux<T> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					if (!range.hasMin() || !range.hasMax()) {
						readOptions.setReadaheadSize(32 * 1024); // 32KiB
						readOptions.setFillCache(false);
					}
					if (logger.isTraceEnabled()) {
						logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(range));
					}
					return getRocksIterator(db.getAllocator(), allowNettyDirect, readOptions, range.copy().send(), db);
				}, (tuple, sink) -> {
					try {
						var rocksIterator = tuple.getT1();
						rocksIterator.status();
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
											LLUtils.toStringSafe(range),
											LLUtils.toStringSafe(key),
											LLUtils.toStringSafe(value)
									);
								}

								try {
									rocksIterator.next();
									rocksIterator.status();
									sink.next(getEntry(key.send(), value == null ? null : value.send()));
								} finally {
									if (value != null) {
										value.close();
									}
								}
							}
						} else {
							if (logger.isTraceEnabled()) {
								logger.trace(MARKER_ROCKSDB, "Range {} ended", LLUtils.toStringSafe(range));
							}
							sink.complete();
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
				});
	}

	public abstract T getEntry(@Nullable Send<Buffer> key, @Nullable Send<Buffer> value);

	@Override
	protected final RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLLocalReactiveRocksIterator<T>> prepareSend() {
		var range = this.range.send();
		var readOptions = new ReadOptions(this.readOptions);
		return drop -> new LLLocalReactiveRocksIterator<>(db, range, allowNettyDirect, readOptions, readValues) {
			@Override
			public T getEntry(@Nullable Send<Buffer> key, @Nullable Send<Buffer> value) {
				return LLLocalReactiveRocksIterator.this.getEntry(key, value);
			}
		};
	}

	protected void makeInaccessible() {
		this.range = null;
		this.readOptions = null;
	}
}
