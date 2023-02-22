package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.generateCustomReadOptions;
import static it.cavallium.dbengine.utils.StreamUtils.streamWhileNonNull;

import it.cavallium.dbengine.buffers.Buf;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
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
	private LLRange range;
	private Supplier<ReadOptions> readOptions;

	public LLLocalMigrationReactiveRocksIterator(RocksDBColumn db,
			LLRange range,
			Supplier<ReadOptions> readOptions) {
		this.db = db;
		this.range = range;
		this.readOptions = readOptions;
	}

	public Stream<LLEntry> stream() {
		var readOptions = generateCustomReadOptions(this.readOptions.get(), false, false, false);
		RocksIteratorObj rocksIterator;
		try {
			rocksIterator = db.newRocksIterator(readOptions, range, false);
		} catch (RocksDBException e) {
			throw new DBException("Failed to open iterator", e);
		}
		return streamWhileNonNull(() -> {
			try {
				if (rocksIterator.isValid()) {
					var key = rocksIterator.keyBuf().copy();
					var value = rocksIterator.valueBuf().copy();
					rocksIterator.next(false);
					return LLEntry.of(key, value);
				} else {
					return null;
				}
			} catch (RocksDBException ex) {
				throw new CompletionException(new DBException("Failed to iterate", ex));
			}
		}).onClose(() -> {
			rocksIterator.close();
			readOptions.close();
		});
	}
}
