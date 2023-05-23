package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.generateCustomReadOptions;
import static it.cavallium.dbengine.utils.StreamUtils.resourceStream;
import static it.cavallium.dbengine.utils.StreamUtils.streamWhileNonNull;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLEntry;
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
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;

public final class LLLocalMigrationReactiveRocksIterator {

	private final RocksDBColumn db;
	private final LLRange range;
	private final Supplier<LLReadOptions> readOptions;

	public LLLocalMigrationReactiveRocksIterator(RocksDBColumn db,
			LLRange range,
			Supplier<LLReadOptions> readOptions) {
		this.db = db;
		this.range = range;
		this.readOptions = readOptions;
	}

	public Stream<LLEntry> stream() {
		try {
			return resourceStream(
					// Create the read options
					() -> generateCustomReadOptions(this.readOptions.get(), false, false, false),
					readOptions -> resourceStream(
							// Create the iterator
							() -> db.newRocksIterator(readOptions, range, false),
							// Stream the iterator values until null is returned
							iterator -> streamWhileNonNull(() -> {
								if (iterator.isValid()) {
									var key = iterator.keyBuf().copy();
									var value = iterator.valueBuf().copy();
									iterator.next(false);
									return LLEntry.of(key, value);
								} else {
									return null;
								}
							})));
		} catch (RocksDBException e) {
			throw new DBException("Failed to open iterator", e);
		}
	}
}
