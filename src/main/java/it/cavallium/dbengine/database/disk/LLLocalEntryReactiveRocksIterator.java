package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.util.Send;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import java.util.function.Supplier;
import org.rocksdb.ReadOptions;
import reactor.core.publisher.Mono;

public class LLLocalEntryReactiveRocksIterator extends LLLocalReactiveRocksIterator<LLEntry> {

	public LLLocalEntryReactiveRocksIterator(RocksDBColumn db,
			Mono<LLRange> rangeMono,
			boolean allowNettyDirect,
			Supplier<ReadOptions> readOptions,
			boolean reverse,
			boolean smallRange) {
		super(db, rangeMono, allowNettyDirect, readOptions, true, reverse, smallRange);
	}

	@Override
	public LLEntry getEntry(Buffer key, Buffer value) {
		assert key != null;
		assert value != null;
		return LLEntry.of(
				key.touch("iteration entry key"),
				value.touch("iteration entry value")
		);
	}
}
