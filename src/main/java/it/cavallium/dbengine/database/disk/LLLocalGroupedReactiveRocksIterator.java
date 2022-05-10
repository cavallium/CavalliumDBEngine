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
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

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
	private final boolean smallRange;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public LLLocalGroupedReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			Send<LLRange> range,
			boolean allowNettyDirect,
			ReadOptions readOptions,
			boolean canFillCache,
			boolean readValues,
			boolean smallRange) {
		super((Drop<LLLocalGroupedReactiveRocksIterator<T>>) (Drop) DROP);
		try (range) {
			this.db = db;
			this.prefixLength = prefixLength;
			this.range = range.receive();
			this.allowNettyDirect = allowNettyDirect;
			this.readOptions = readOptions;
			this.canFillCache = canFillCache;
			this.readValues = readValues;
			this.smallRange = smallRange;
		}
	}


	public final Flux<List<T>> flux() {
		return Flux.generate(() -> {
			var readOptions = generateCustomReadOptions(this.readOptions, true, isBoundedRange(range), smallRange);
			if (logger.isTraceEnabled()) {
				logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(range));
			}
			return Tuples.of(readOptions, db.getRocksIterator(allowNettyDirect, readOptions, range, false));
		}, (tuple, sink) -> {
			try {
				var rocksIterator = tuple.getT2().iterator();
				ObjectArrayList<T> values = new ObjectArrayList<>();
				Buffer firstGroupKey = null;
				try {
					while (rocksIterator.isValid()) {
						try (Buffer key = LLUtils.readDirectNioBuffer(db.getAllocator(), rocksIterator::key)) {
							if (firstGroupKey == null) {
								firstGroupKey = key.copy();
							} else if (!LLUtils.equals(firstGroupKey,
									firstGroupKey.readerOffset(),
									key,
									key.readerOffset(),
									prefixLength
							)) {
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
		}, t -> {
			t.getT2().close();
			t.getT1().close();
			this.close();
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
		var readOptions = this.readOptions;
		return drop -> new LLLocalGroupedReactiveRocksIterator<>(db,
				prefixLength,
				range,
				allowNettyDirect,
				readOptions,
				canFillCache,
				readValues,
				smallRange
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
