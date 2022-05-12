package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.generateCustomReadOptions;

import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.Send;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.disk.rocksdb.RocksObj;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

public final class LLLocalMigrationReactiveRocksIterator extends
		ResourceSupport<LLLocalMigrationReactiveRocksIterator, LLLocalMigrationReactiveRocksIterator> {

	private static final Logger logger = LogManager.getLogger(LLLocalMigrationReactiveRocksIterator.class);
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
					obj.readOptions.close();
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
	private RocksObj<ReadOptions> readOptions;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public LLLocalMigrationReactiveRocksIterator(RocksDBColumn db,
			Send<LLRange> range,
			Send<RocksObj<ReadOptions>> readOptions) {
		super((Drop<LLLocalMigrationReactiveRocksIterator>) (Drop) DROP);
		try (range) {
			this.db = db;
			this.rangeShared = range.receive();
			this.readOptions = readOptions.receive();
		}
	}

	public record ByteEntry(byte[] key, byte[] value) {}

	public Flux<ByteEntry> flux() {
		return Flux.generate(() -> {
			var readOptions = generateCustomReadOptions(this.readOptions, false, false, false);
			return new RocksIterWithReadOpts(readOptions, db.newRocksIterator(false, readOptions, rangeShared, false));
		}, (tuple, sink) -> {
			try {
				var rocksIterator = tuple.iter();
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
		}, RocksIterWithReadOpts::close);
	}

	@Override
	protected final RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<LLLocalMigrationReactiveRocksIterator> prepareSend() {
		var range = this.rangeShared.send();
		var readOptions = this.readOptions.send();
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
