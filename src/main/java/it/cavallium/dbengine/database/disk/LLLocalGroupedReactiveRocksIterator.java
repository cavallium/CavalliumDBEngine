package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.generateCustomReadOptions;
import static it.cavallium.dbengine.database.LLUtils.isBoundedRange;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.rocksdb.LLReadOptions;
import it.cavallium.dbengine.database.disk.rocksdb.RocksIteratorObj;
import it.cavallium.dbengine.utils.DBException;
import it.cavallium.dbengine.utils.StreamUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;

public abstract class LLLocalGroupedReactiveRocksIterator<T> {

	protected static final Logger logger = LogManager.getLogger(LLLocalGroupedReactiveRocksIterator.class);

	private final RocksDBColumn db;
	private final int prefixLength;
	private final LLRange range;
	private final Supplier<LLReadOptions> readOptions;
	private final boolean canFillCache;
	private final boolean readValues;
	private final boolean smallRange;

	public LLLocalGroupedReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			LLRange range,
			Supplier<LLReadOptions> readOptions,
			boolean canFillCache,
			boolean readValues,
			boolean smallRange) {
		this.db = db;
		this.prefixLength = prefixLength;
		this.range = range;
		this.readOptions = readOptions != null ? readOptions : LLReadOptions::new;
		this.canFillCache = canFillCache;
		this.readValues = readValues;
		this.smallRange = smallRange;
	}

	public final Stream<List<T>> stream() {
		var readOptions = generateCustomReadOptions(this.readOptions.get(), canFillCache, isBoundedRange(range), smallRange);
		if (logger.isTraceEnabled()) {
			logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(range));
		}

		RocksIteratorObj rocksIterator;
		try {
			rocksIterator = db.newRocksIterator(readOptions, range, false);
		} catch (RocksDBException e) {
			readOptions.close();
			throw new DBException("Failed to iterate the range", e);
		}

		return StreamUtils.<List<T>>streamWhileNonNull(() -> {
			try {
				ObjectArrayList<T> values = new ObjectArrayList<>();
				Buf firstGroupKey = null;
				while (rocksIterator.isValid()) {
					// Note that the underlying array is subject to changes!
					Buf key = rocksIterator.keyBuf();
					if (firstGroupKey == null) {
						firstGroupKey = key.copy();
					} else if (!LLUtils.equals(firstGroupKey, 0, key, 0, prefixLength)) {
						break;
					}
					// Note that the underlying array is subject to changes!
					@Nullable Buf value;
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

					rocksIterator.next();
					T entry = getEntry(key, value);
					values.add(entry);
				}
				if (!values.isEmpty()) {
					return values;
				} else {
					if (logger.isTraceEnabled()) {
						logger.trace(MARKER_ROCKSDB, "Range {} ended", LLUtils.toStringSafe(range));
					}
					return null;
				}
			} catch (RocksDBException ex) {
				if (logger.isTraceEnabled()) {
					logger.trace(MARKER_ROCKSDB, "Range {} failed", LLUtils.toStringSafe(range));
				}
				throw new CompletionException(new DBException("Range failed", ex));
			}
		}).onClose(() -> {
			rocksIterator.close();
			readOptions.close();
		});
	}

	/**
	 * @param key this buffer content will be changed during the next iteration
	 * @param value this buffer content will be changed during the next iteration
	 */
	public abstract T getEntry(@Nullable Buf key, @Nullable Buf value);

}
