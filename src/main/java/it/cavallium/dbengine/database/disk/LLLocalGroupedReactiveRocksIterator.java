package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.Drop;
import io.net5.buffer.api.Owned;
import io.net5.buffer.api.Send;
import io.net5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;

public abstract class LLLocalGroupedReactiveRocksIterator<T> extends
		ResourceSupport<LLLocalGroupedReactiveRocksIterator<T>, LLLocalGroupedReactiveRocksIterator<T>> {

	protected static final Logger logger = LoggerFactory.getLogger(LLLocalGroupedReactiveRocksIterator.class);
	private static final Drop<LLLocalGroupedReactiveRocksIterator<?>> DROP = new Drop<>() {
		@Override
		public void drop(LLLocalGroupedReactiveRocksIterator<?> obj) {
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
		public Drop<LLLocalGroupedReactiveRocksIterator<?>> fork() {
			return this;
		}

		@Override
		public void attach(LLLocalGroupedReactiveRocksIterator<?> obj) {

		}
	};

	private final RocksDBColumn db;
	private final int prefixLength;
	private LLRange range;
	private final boolean allowNettyDirect;
	private ReadOptions readOptions;
	private final boolean canFillCache;
	private final boolean readValues;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public LLLocalGroupedReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			Send<LLRange> range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean canFillCache,
			boolean readValues) {
		super((Drop<LLLocalGroupedReactiveRocksIterator<T>>) (Drop) DROP);
		try (range) {
			this.db = db;
			this.prefixLength = prefixLength;
			this.range = range.receive();
			this.allowNettyDirect = allowNettyDirect;
			this.readOptions = readOptions;
			this.canFillCache = canFillCache;
			this.readValues = readValues;
		}
	}


	public final Flux<List<T>> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					readOptions.setFillCache(canFillCache && range.hasMin() && range.hasMax());
					if (logger.isTraceEnabled()) {
						logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(range));
					}
					return LLLocalDictionary.getRocksIterator(db.getAllocator(), allowNettyDirect, readOptions, range.copy().send(), db);
				}, (tuple, sink) -> {
					try {
						var rocksIterator = tuple.getT1();
						ObjectArrayList<T> values = new ObjectArrayList<>();
						Buffer firstGroupKey = null;
						try {
							rocksIterator.status();
							while (rocksIterator.isValid()) {
								try (Buffer key = LLUtils.readDirectNioBuffer(db.getAllocator(), rocksIterator::key)) {
									if (firstGroupKey == null) {
										firstGroupKey = key.copy();
									} else if (!LLUtils.equals(firstGroupKey, firstGroupKey.readerOffset(),
											key, key.readerOffset(), prefixLength)) {
										break;
									}
									@Nullable Buffer value;
									if (readValues) {
										value = LLUtils.readDirectNioBuffer(db.getAllocator(), rocksIterator::value);
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
										T entry = getEntry(key.send(), value == null ? null : value.send());
										values.add(entry);
									} finally {
										if (value != null) {
											value.close();
										}
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
	protected Owned<LLLocalGroupedReactiveRocksIterator<T>> prepareSend() {
		var range = this.range.send();
		var readOptions = new ReadOptions(this.readOptions);
		return drop -> new LLLocalGroupedReactiveRocksIterator<>(db,
				prefixLength,
				range,
				allowNettyDirect,
				readOptions,
				canFillCache,
				readValues
		) {
			@Override
			public T getEntry(@Nullable Send<Buffer> key, @Nullable Send<Buffer> value) {
				return LLLocalGroupedReactiveRocksIterator.this.getEntry(key, value);
			}
		};
	}

	protected void makeInaccessible() {
		this.range = null;
		this.readOptions = null;
	}
}
