package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.generateCustomReadOptions;
import static it.cavallium.dbengine.database.LLUtils.isClosedRange;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.Send;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;

public abstract class LLLocalGroupedReactiveRocksIterator<T> extends
		ResourceSupport<LLLocalGroupedReactiveRocksIterator<T>, LLLocalGroupedReactiveRocksIterator<T>> {

	protected static final Logger logger = LogManager.getLogger(LLLocalGroupedReactiveRocksIterator.class);
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
					if (!(obj.readOptions instanceof UnreleasableReadOptions)) {
						obj.readOptions.close();
					}
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
					var readOptions = generateCustomReadOptions(this.readOptions, true, isClosedRange(range), true);
					if (logger.isTraceEnabled()) {
						logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(range));
					}
					return LLLocalDictionary.getRocksIterator(allowNettyDirect, readOptions, range, db, false);
				}, (tuple, sink) -> {
					try {
						var rocksIterator = tuple.iterator();
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
				}, RocksIteratorTuple::close);
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
