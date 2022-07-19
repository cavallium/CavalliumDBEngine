package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.util.Send;
import it.cavallium.dbengine.database.LLEntry;
import it.cavallium.dbengine.database.LLRange;
import java.util.function.Supplier;
import org.rocksdb.ReadOptions;
import reactor.core.publisher.Mono;

public class LLLocalGroupedEntryReactiveRocksIterator extends
		LLLocalGroupedReactiveRocksIterator<LLEntry> {

	public LLLocalGroupedEntryReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			Mono<LLRange> rangeMono,
			boolean allowNettyDirect,
			Supplier<ReadOptions> readOptions,
			boolean smallRange) {
		super(db, prefixLength, rangeMono, allowNettyDirect, readOptions, false, true, smallRange);
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
