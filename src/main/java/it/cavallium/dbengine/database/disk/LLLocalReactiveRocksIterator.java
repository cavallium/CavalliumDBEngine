package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.generateCustomReadOptions;
import static it.cavallium.dbengine.database.LLUtils.isBoundedRange;

import io.netty5.buffer.Buffer;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class LLLocalReactiveRocksIterator<T> {

	protected static final Logger logger = LogManager.getLogger(LLLocalReactiveRocksIterator.class);

	private final RocksDBColumn db;
	private final Mono<LLRange> rangeMono;
	private final boolean allowNettyDirect;
	private final Supplier<ReadOptions> readOptions;
	private final boolean readValues;
	private final boolean reverse;
	private final boolean smallRange;

	public LLLocalReactiveRocksIterator(RocksDBColumn db,
			Mono<LLRange> rangeMono,
			boolean allowNettyDirect,
			Supplier<ReadOptions> readOptions,
			boolean readValues,
			boolean reverse,
			boolean smallRange) {
		this.db = db;
		this.rangeMono = rangeMono;
		this.allowNettyDirect = allowNettyDirect;
		this.readOptions = readOptions != null ? readOptions : ReadOptions::new;
		this.readValues = readValues;
		this.reverse = reverse;
		this.smallRange = smallRange;
	}

	public final Flux<T> flux() {
		return Flux.usingWhen(rangeMono, range -> Flux.generate(() -> {
			var readOptions = generateCustomReadOptions(this.readOptions.get(), true, isBoundedRange(range), smallRange);
			if (logger.isTraceEnabled()) {
				logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(range));
			}
			return new RocksIterWithReadOpts(readOptions, db.newRocksIterator(allowNettyDirect, readOptions, range, reverse));
		}, (tuple, sink) -> {
			try {
				var rocksIterator = tuple.iter();
				if (rocksIterator.isValid()) {
					Buffer key;
					if (allowNettyDirect) {
						key = LLUtils.readDirectNioBuffer(db.getAllocator(), rocksIterator::key);
					} else {
						key = LLUtils.fromByteArray(db.getAllocator(), rocksIterator.key());
					}
					try {
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
							if (reverse) {
								rocksIterator.prev();
							} else {
								rocksIterator.next();
							}
							sink.next(getEntry(key, value));
						} catch (Throwable ex) {
							if (value != null && value.isAccessible()) {
								try {
									value.close();
								} catch (Throwable ex2) {
									logger.error(ex2);
								}
							}
							throw ex;
						}
					} catch (Throwable ex) {
						if (key.isAccessible()) {
							try {
								key.close();
							} catch (Throwable ex2) {
								logger.error(ex2);
							}
						}
						throw ex;
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
		}, RocksIterWithReadOpts::close), LLUtils::finalizeResource);
	}

	public abstract T getEntry(@Nullable Buffer key, @Nullable Buffer value);

}
