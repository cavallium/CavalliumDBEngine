package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.generateCustomReadOptions;

import io.netty5.buffer.Drop;
import io.netty5.buffer.Owned;
import io.netty5.util.Send;
import io.netty5.buffer.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.utils.SimpleResource;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

public final class LLLocalMigrationReactiveRocksIterator {

	private final RocksDBColumn db;
	private Mono<LLRange> rangeMono;
	private Supplier<ReadOptions> readOptions;

	public LLLocalMigrationReactiveRocksIterator(RocksDBColumn db,
			Mono<LLRange> rangeMono,
			Supplier<ReadOptions> readOptions) {
		this.db = db;
		this.rangeMono = rangeMono;
		this.readOptions = readOptions;
	}

	public record ByteEntry(byte[] key, byte[] value) {}

	public Flux<ByteEntry> flux() {
		return Flux.usingWhen(rangeMono, range -> Flux.generate(() -> {
			var readOptions = generateCustomReadOptions(this.readOptions.get(), false, false, false);
			return new RocksIterWithReadOpts(readOptions, db.newRocksIterator(false, readOptions, range, false));
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
		}, RocksIterWithReadOpts::close), LLUtils::finalizeResource);
	}
}
