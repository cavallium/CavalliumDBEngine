package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.util.Send;
import it.cavallium.dbengine.database.LLRange;
import java.util.function.Supplier;
import org.rocksdb.ReadOptions;
import reactor.core.publisher.Mono;

public class LLLocalGroupedKeyReactiveRocksIterator extends LLLocalGroupedReactiveRocksIterator<Buffer> {

	public LLLocalGroupedKeyReactiveRocksIterator(RocksDBColumn db,
			int prefixLength,
			Mono<LLRange> rangeMono,
			boolean allowNettyDirect,
			Supplier<ReadOptions> readOptions,
			boolean smallRange) {
		super(db, prefixLength, rangeMono, allowNettyDirect, readOptions, true, false, smallRange);
	}

	@Override
	public Buffer getEntry(Buffer key, Buffer value) {
		if (value != null) {
			value.close();
		}
		return key;
	}
}
