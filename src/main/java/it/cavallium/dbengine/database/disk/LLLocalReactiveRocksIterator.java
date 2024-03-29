package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.generateCustomReadOptions;
import static it.cavallium.dbengine.database.LLUtils.isBoundedRange;
import static it.cavallium.dbengine.utils.StreamUtils.resourceStream;
import static it.cavallium.dbengine.utils.StreamUtils.streamWhileNonNull;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.rocksdb.LLReadOptions;
import it.cavallium.dbengine.database.disk.rocksdb.RocksIteratorObj;
import it.cavallium.dbengine.utils.DBException;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;

public abstract class LLLocalReactiveRocksIterator<T> {

	protected static final Logger logger = LogManager.getLogger(LLLocalReactiveRocksIterator.class);

	private final RocksDBColumn db;
	private final LLRange range;
	private final Supplier<LLReadOptions> readOptions;
	private final boolean readValues;
	private final boolean reverse;
	private final boolean smallRange;

	public LLLocalReactiveRocksIterator(RocksDBColumn db,
			LLRange range,
			Supplier<LLReadOptions> readOptions,
			boolean readValues,
			boolean reverse,
			boolean smallRange) {
		this.db = db;
		this.range = range;
		this.readOptions = readOptions != null ? readOptions : LLReadOptions::new;
		this.readValues = readValues;
		this.reverse = reverse;
		this.smallRange = smallRange;
	}

	public final Stream<T> stream() {
		try {
			return resourceStream(
					() -> generateCustomReadOptions(this.readOptions.get(), true, isBoundedRange(range), smallRange),
					readOptions -> resourceStream(
							() -> db.newRocksIterator(readOptions, range, reverse),
							rocksIterator -> streamWhileNonNull(() -> {
								try {
									if (logger.isTraceEnabled()) {
										logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(range));
									}
									if (rocksIterator.isValid()) {
										// Note that the underlying array is subject to changes!
										Buf key;
										key = rocksIterator.keyBuf();
										// Note that the underlying array is subject to changes!
										Buf value;
										if (readValues) {
											value = rocksIterator.valueBuf();
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

										if (reverse) {
											rocksIterator.prev();
										} else {
											rocksIterator.next();
										}
										return getEntry(key, value);
									} else {
										if (logger.isTraceEnabled()) {
											logger.trace(MARKER_ROCKSDB, "Range {} ended", LLUtils.toStringSafe(range));
										}
										return null;
									}
								} catch (RocksDBException ex) {
									throw new CompletionException(generateRangeFailedException(ex));
								}
							})
					)
			);
		} catch (RocksDBException ex) {
			throw generateRangeFailedException(ex);
		}
	}

	private DBException generateRangeFailedException(RocksDBException ex) {
		if (logger.isTraceEnabled()) {
			logger.trace(MARKER_ROCKSDB, "Range {} failed", LLUtils.toStringSafe(range));
		}
		throw new DBException("Range failed", ex);
	}

	/**
	 * @param key this buffer content will be changed during the next iteration
	 * @param value this buffer content will be changed during the next iteration
	 */
	public abstract T getEntry(@Nullable Buf key, @Nullable Buf value);

}
