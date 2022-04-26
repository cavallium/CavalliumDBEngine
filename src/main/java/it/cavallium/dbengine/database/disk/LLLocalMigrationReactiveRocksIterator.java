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
import it.cavallium.dbengine.database.disk.LLLocalMigrationReactiveRocksIterator.ByteEntry;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;

public final class LLLocalMigrationReactiveRocksIterator extends
		ResourceSupport<LLLocalMigrationReactiveRocksIterator, LLLocalMigrationReactiveRocksIterator> {

	protected static final Logger logger = LogManager.getLogger(LLLocalMigrationReactiveRocksIterator.class);
	private static final Drop<LLLocalMigrationReactiveRocksIterator> DROP = new Drop<>() {
		@Override
		public void drop(LLLocalMigrationReactiveRocksIterator obj) {
			try {
				if (obj.rangeShared != null) {
					obj.rangeShared.close();
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
		public Drop<LLLocalMigrationReactiveRocksIterator> fork() {
			return this;
		}

		@Override
		public void attach(LLLocalMigrationReactiveRocksIterator obj) {

		}
	};

	private final RocksDBColumn db;
	private LLRange rangeShared;
	private ReadOptions readOptions;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public LLLocalMigrationReactiveRocksIterator(RocksDBColumn db,
			Send<LLRange> range,
			ReadOptions readOptions) {
		super((Drop<LLLocalMigrationReactiveRocksIterator>) (Drop) DROP);
		try (range) {
			this.db = db;
			this.rangeShared = range.receive();
			this.readOptions = readOptions;
		}
	}

	public record ByteEntry(byte[] key, byte[] value) {}

	public Flux<ByteEntry> flux() {
		return Flux.generate(() -> {
			var readOptions = generateCustomReadOptions(this.readOptions, false, false, false);
			return db.getRocksIterator(false, readOptions, rangeShared, false);
		}, (tuple, sink) -> {
			try {
				var rocksIterator = tuple.iterator();
				if (rocksIterator.isValid()) {
					byte[] key = rocksIterator.key();
					byte[] value = rocksIterator.value();
					rocksIterator.next(false);
					sink.next(new ByteEntry(key, value));
				} else {
					sink.complete();
				}
			} catch (RocksDBException ex) {
				sink.error(ex);
			}
			return tuple;
		}, RocksIteratorTuple::close);
	}

	@Override
	protected final RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLLocalMigrationReactiveRocksIterator> prepareSend() {
		var range = this.rangeShared.send();
		var readOptions = this.readOptions;
		return drop -> new LLLocalMigrationReactiveRocksIterator(db,
				range,
				readOptions
		);
	}

	protected void makeInaccessible() {
		this.rangeShared = null;
		this.readOptions = null;
	}
}
