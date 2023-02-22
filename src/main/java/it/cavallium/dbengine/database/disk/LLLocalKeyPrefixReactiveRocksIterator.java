package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.MARKER_ROCKSDB;
import static it.cavallium.dbengine.database.LLUtils.generateCustomReadOptions;
import static it.cavallium.dbengine.database.LLUtils.isBoundedRange;
import static it.cavallium.dbengine.utils.StreamUtils.streamWhileNonNull;

import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.utils.DBException;
import it.cavallium.dbengine.utils.StreamUtils;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;

public class LLLocalKeyPrefixReactiveRocksIterator {

	protected static final Logger logger = LogManager.getLogger(LLLocalKeyPrefixReactiveRocksIterator.class);

	private final RocksDBColumn db;
	private final int prefixLength;
	private final LLRange range;
	private final Supplier<ReadOptions> readOptions;
	private final boolean canFillCache;
	private final boolean smallRange;

	public LLLocalKeyPrefixReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			LLRange range,
			Supplier<ReadOptions> readOptions,
			boolean canFillCache,
			boolean smallRange) {
		this.db = db;
		this.prefixLength = prefixLength;
		this.range = range;
		this.readOptions = readOptions != null ? readOptions : ReadOptions::new;
		this.canFillCache = canFillCache;
		this.smallRange = smallRange;
	}


	public Stream<Buf> stream() {
		try {
			var readOptions
					= generateCustomReadOptions(this.readOptions.get(), canFillCache, isBoundedRange(range), smallRange);
			if (logger.isTraceEnabled()) {
				logger.trace(MARKER_ROCKSDB, "Range {} started", LLUtils.toStringSafe(range));
			}
			var rocksIterator = db.newRocksIterator(readOptions, range, false);

			return streamWhileNonNull(() -> {
				try {
					Buf firstGroupKey = null;
					while (rocksIterator.isValid()) {
						// Note that the underlying array is subject to changes!
						Buf key = rocksIterator.keyBuf();
						var keyLen = key.size();
						if (keyLen >= prefixLength) {
							if (firstGroupKey == null) {
								firstGroupKey = key.copy();
								assert firstGroupKey == null || firstGroupKey.size() >= prefixLength;
							} else if (!LLUtils.equals(firstGroupKey,
									0,
									key,
									0,
									prefixLength
							)) {
								break;
							}
						} else {
							logger.error("Skipped a key with length {}, the expected minimum prefix key length is {}!"
									+ " This key will be dropped", key.size(), prefixLength);
						}
						rocksIterator.next();
					}

					if (firstGroupKey != null) {
						var groupKeyPrefix = firstGroupKey.subList(0, prefixLength);

						if (logger.isTraceEnabled()) {
							logger.trace(MARKER_ROCKSDB,
									"Range {} is reading prefix {}",
									LLUtils.toStringSafe(range),
									LLUtils.toStringSafe(groupKeyPrefix)
							);
						}

						return groupKeyPrefix;
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
		} catch (RocksDBException e) {
			throw new DBException("Failed to open stream", e);
		}
	}

}
