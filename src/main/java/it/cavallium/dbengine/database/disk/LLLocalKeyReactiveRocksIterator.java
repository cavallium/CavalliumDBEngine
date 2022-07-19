package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.util.Send;
import it.cavallium.dbengine.database.LLRange;
import java.util.function.Supplier;
import org.rocksdb.ReadOptions;
import reactor.core.publisher.Mono;

public class LLLocalKeyReactiveRocksIterator extends LLLocalReactiveRocksIterator<Buffer> {

	public LLLocalKeyReactiveRocksIterator(RocksDBColumn db,
			Mono<LLRange> rangeMono,
			boolean allowNettyDirect,
			Supplier<ReadOptions> readOptions,
			boolean reverse,
			boolean smallRange) {
		super(db, rangeMono, allowNettyDirect, readOptions, false, reverse, smallRange);
	}

	@Override
	public Buffer getEntry(Buffer key, Buffer value) {
		if (value != null) {
			value.close();
		}
		return key;
	}
}
