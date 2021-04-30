package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.disk.LLLocalDictionary.getRocksIterator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.List;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksMutableObject;
import reactor.core.publisher.Flux;
import static io.netty.buffer.Unpooled.*;

public abstract class LLLocalGroupedReactiveRocksIterator<T> {

	private final RocksDB db;
	private final ByteBufAllocator alloc;
	private final ColumnFamilyHandle cfh;
	private final int prefixLength;
	private final LLRange range;
	private final ReadOptions readOptions;
	private final boolean canFillCache;
	private final boolean readValues;

	public LLLocalGroupedReactiveRocksIterator(RocksDB db, ByteBufAllocator alloc, ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			ReadOptions readOptions,
			boolean canFillCache,
			boolean readValues) {
		this.db = db;
		this.alloc = alloc;
		this.cfh = cfh;
		this.prefixLength = prefixLength;
		this.range = range;
		this.readOptions = readOptions;
		this.canFillCache = canFillCache;
		this.readValues = readValues;
	}


	public Flux<List<T>> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					readOptions.setFillCache(canFillCache && range.hasMin() && range.hasMax());
					return getRocksIterator(readOptions, range, db, cfh);
				}, (tuple, sink) -> {
					var rocksIterator = tuple.getT1();
					ObjectArrayList<T> values = new ObjectArrayList<>();
					ByteBuf firstGroupKey = null;

					try {
						while (rocksIterator.isValid()) {
							ByteBuf key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
							try {
								if (firstGroupKey == null) {
									firstGroupKey = key.retainedSlice();
								} else if (!ByteBufUtil.equals(firstGroupKey, 0, key, 0, prefixLength)) {
									break;
								}
								ByteBuf value = readValues ? LLUtils.readDirectNioBuffer(alloc, rocksIterator::value) : EMPTY_BUFFER;
								try {
									rocksIterator.next();
									T entry = getEntry(key.retain(), value.retain());
									values.add(entry);
								} finally {
									value.release();
								}
							} finally {
								key.release();
							}
						}
					} finally {
						if (firstGroupKey != null) {
							firstGroupKey.release();
						}
					}
					if (!values.isEmpty()) {
						sink.next(values);
					} else {
						sink.complete();
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

	public abstract T getEntry(ByteBuf key, ByteBuf value);
}
