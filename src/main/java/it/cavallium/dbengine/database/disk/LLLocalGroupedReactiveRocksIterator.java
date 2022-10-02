package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.generateCustomReadOptions;
import static it.cavallium.dbengine.database.LLUtils.isBoundedRange;

import io.netty5.buffer.Buffer;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class LLLocalGroupedReactiveRocksIterator<T> {

	protected static final Logger logger = LogManager.getLogger(LLLocalGroupedReactiveRocksIterator.class);

	private final RocksDBColumn db;
	private final int prefixLength;
	private final Mono<LLRange> rangeMono;
	private final boolean allowNettyDirect;
	private final Supplier<ReadOptions> readOptions;
	private final boolean canFillCache;
	private final boolean readValues;
	private final boolean smallRange;

	public LLLocalGroupedReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			Mono<LLRange> rangeMono,
			boolean allowNettyDirect,
			Supplier<ReadOptions> readOptions,
			boolean canFillCache,
			boolean readValues,
			boolean smallRange) {
		this.db = db;
		this.prefixLength = prefixLength;
		this.rangeMono = rangeMono;
		this.allowNettyDirect = allowNettyDirect;
		this.readOptions = readOptions != null ? readOptions : ReadOptions::new;
		this.canFillCache = canFillCache;
		this.readValues = readValues;
		this.smallRange = smallRange;
	}

	public final Flux<List<T>> flux() {
		return Flux.usingWhen(rangeMono, range -> Flux.generate(() -> {
			var readOptions = generateCustomReadOptions(this.readOptions.get(), true, isBoundedRange(range), smallRange);
			if (logger.isTraceEnabled()) {
				logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(range));
			}
			return new RocksIterWithReadOpts(readOptions, db.newRocksIterator(allowNettyDirect, readOptions, range, false));
		}, (tuple, sink) -> {
			try {
				var rocksIterator = tuple.iter();
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
								T entry = getEntry(key, value);
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
		}, RocksIterWithReadOpts::close), LLUtils::finalizeResource);
	}

	public abstract T getEntry(@Nullable Buffer key, @Nullable Buffer value);

}
