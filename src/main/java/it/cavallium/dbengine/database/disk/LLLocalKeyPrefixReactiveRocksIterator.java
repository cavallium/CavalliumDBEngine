package it.cavallium.dbengine.database.disk;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import java.util.Arrays;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksMutableObject;
import reactor.core.publisher.Flux;
import static io.netty.buffer.Unpooled.*;

public class LLLocalKeyPrefixReactiveRocksIterator {

	private final RocksDB db;
	private final ByteBufAllocator alloc;
	private final ColumnFamilyHandle cfh;
	private final int prefixLength;
	private final LLRange range;
	private final ReadOptions readOptions;
	private final boolean canFillCache;
	private final String debugName;

	public LLLocalKeyPrefixReactiveRocksIterator(RocksDB db, ByteBufAllocator alloc, ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			ReadOptions readOptions,
			boolean canFillCache,
			String debugName) {
		this.db = db;
		this.alloc = alloc;
		this.cfh = cfh;
		this.prefixLength = prefixLength;
		this.range = range;
		this.readOptions = readOptions;
		this.canFillCache = canFillCache;
		this.debugName = debugName;
	}


	public Flux<ByteBuf> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					if (!range.hasMin() || !range.hasMax()) {
						//readOptions.setReadaheadSize(2 * 1024 * 1024);
						readOptions.setFillCache(canFillCache);
					}
					return LLLocalDictionary.getRocksIterator(readOptions, range.retain(), db, cfh);
				}, (tuple, sink) -> {
					var rocksIterator = tuple.getT1();
					ByteBuf firstGroupKey = null;
					try {
						while (rocksIterator.isValid()) {
							ByteBuf key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
							try {
								if (firstGroupKey == null) {
									firstGroupKey = key.retain();
								} else if (!ByteBufUtil.equals(firstGroupKey, 0, key, 0, prefixLength)) {
									break;
								}
								rocksIterator.next();
							} finally {
								key.release();
							}
						}
						if (firstGroupKey != null) {
							var groupKeyPrefix = firstGroupKey.slice(0, prefixLength);
							sink.next(groupKeyPrefix.retain());
						} else {
							sink.complete();
						}
					} finally {
						if (firstGroupKey != null) {
							firstGroupKey.release();
						}
					}
					return tuple;
				}, tuple -> {
					var rocksIterator = tuple.getT1();
					rocksIterator.close();
					tuple.getT2().release();
					tuple.getT3().release();
					range.release();
				});
	}

}
